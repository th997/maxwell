package com.zendesk.maxwell.producer.jdbc;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.BinlogDelayGaugeSet;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

public class JdbcProducer extends AbstractProducer implements StoppableTask {
	protected final Logger LOG = LoggerFactory.getLogger(getClass());
	final MaxwellContext context;
	final Properties properties;
	final ComboPooledDataSource targetDs;
	final JdbcTemplate targetJdbcTemplate;
	final JdbcTemplate mysqlJdbcTemplate;
	final DataSourceTransactionManager targetTransactionManager;
	final DataSourceTransactionManager mysqlTransactionManager;
	final TableSyncLogic tableSyncLogic;
	DorisLogic dorisLogic;
	final ConcurrentMap<Thread, Deque<UpdateSql>> sqlListMap = new ConcurrentHashMap();
	final ConcurrentMap<Thread, Long> lastUpdateMap = new ConcurrentHashMap();
	final Set<String> syncDbs;
	final Map<String, String> syncDbMap;
	String type;
	final Integer batchLimit;
	final Integer batchTransactionLimit;
	final Integer sqlMergeSize;
	Integer maxPoolSize;
	final Integer syncIndexMinute;
	final Integer heartbeatSecond;
	final Integer sqlArgsLimit;
	final Integer replicationNum;
	final Integer bucketNum;
	final boolean initSchemas;
	final boolean resolvePkConflict;
	// init data
	final boolean initData;
	final boolean initDataLock;
	final Integer initDataThreadNum;
	final boolean initDataDelete;
	final ObjectMapper om;
	Position initPosition = null;
	// data compare
	final Integer dataCompareSecond;
	DataCompareLogic dataCompareLogic;
	Integer dataCompareLimit;
	Integer dataCompareRowsLimit;

	public JdbcProducer(MaxwellContext context) throws IOException {
		super(context);
		this.context = context;
		om = new ObjectMapper();
		om.registerModule(new JavaTimeModule());
		om.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
		properties = context.getConfig().jdbcProperties;
		resolvePkConflict = "true".equalsIgnoreCase(properties.getProperty("resolvePkConflict", "true"));
		syncDbs = new HashSet<>(Arrays.asList(StringUtils.split(properties.getProperty("syncDbs", ""), ",")));
		syncDbMap = om.readValue(properties.getProperty("syncDbMap", "{}"), new TypeReference<HashMap<String, String>>() {
		});
		initSchemas = "true".equalsIgnoreCase(properties.getProperty("initSchemas"));
		batchLimit = Integer.parseInt(properties.getProperty("batchLimit", "1000"));
		batchTransactionLimit = Integer.parseInt(properties.getProperty("batchTransactionLimit", "160000"));
		sqlMergeSize = Integer.parseInt(properties.getProperty("sqlMergeSize", "5"));
		maxPoolSize = Integer.parseInt(properties.getProperty("maxPoolSize", "10"));
		syncIndexMinute = Integer.parseInt(properties.getProperty("syncIndexMinute", "600"));
		sqlArgsLimit = Integer.parseInt(properties.getProperty("sqlArgsLimit", "65536"));
		replicationNum = Integer.parseInt(properties.getProperty("replicationNum", "1"));
		bucketNum = Integer.parseInt(properties.getProperty("bucketNum", "1"));
		heartbeatSecond = Integer.parseInt(properties.getProperty("heartbeatSecond", "10"));
		dataCompareSecond = Integer.parseInt(properties.getProperty("dataCompareSecond", "0"));
		// init data
		initData = "true".equalsIgnoreCase(properties.getProperty("initData"));
		initDataLock = "true".equalsIgnoreCase(properties.getProperty("initDataLock", "true"));
		initDataThreadNum = Integer.parseInt(properties.getProperty("initDataThreadNum", "10"));
		initDataDelete = "true".equalsIgnoreCase(properties.getProperty("initDataDelete"));
		if (initData) {
			maxPoolSize = Math.max(initDataThreadNum, maxPoolSize);
		}
		String url = properties.getProperty("url");
		type = properties.getProperty("type");
		if (StringUtils.isEmpty(type)) {
			type = StringUtils.split(url, ":")[1].toLowerCase();
		}
		context.getConfig().outputConfig.byte2base64 = isDoris();
		targetDs = new ComboPooledDataSource();
		targetDs.setJdbcUrl(url);
		targetDs.setUser(properties.getProperty("user"));
		targetDs.setPassword(properties.getProperty("password"));
		targetDs.setTestConnectionOnCheckout(true);
		targetDs.setMinPoolSize(1);
		targetDs.setMaxPoolSize(maxPoolSize);
		targetDs.setMaxIdleTime(180);
		targetDs.setPreferredTestQuery("select 1");
		targetDs.setAcquireRetryAttempts(Integer.MAX_VALUE);
		targetDs.setAcquireRetryDelay(3000);
		targetJdbcTemplate = new JdbcTemplate(targetDs, true);
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getReplicationConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.getCpds(), true);
		targetTransactionManager = new DataSourceTransactionManager(targetDs);
		mysqlTransactionManager = new DataSourceTransactionManager(sourcePool.getCpds());
		tableSyncLogic = new TableSyncLogic(this);
		if (isDoris()) {
			dorisLogic = new DorisLogic(this);
		}
		if (syncDbs.size() == 1 && syncDbs.contains("all")) {
			syncDbs.clear();
			syncDbs.addAll(tableSyncLogic.getDbs());
		}
		if (!context.getConfig().producerType.equals("async_jdbc")) {
			start();
		}
	}

	public void start() {
		if (initSchemas) {
			this.initSchemas(syncDbs, initData);
			if (initData) {
				LOG.info("InitSchemas completed!!! The program will exit!!! please set config initSchemas=false and restart,initPosition={}", initPosition);
				if (initPosition != null) {
					try {
						context.getPositionStore().set(initPosition);
					} catch (Exception e) {
						LOG.error("init error", e);
					}
				}
				System.exit(0);
			}
		}
		if (syncIndexMinute > 0) {
			this.startSyncIndexTask(syncIndexMinute);
		}
		if (heartbeatSecond > 0) {
			this.startHeartbeatTask();
		}
		if (dataCompareSecond > 0) {
			dataCompareLogic = new DataCompareLogic(this);
			dataCompareLimit = Integer.parseInt(properties.getProperty("dataCompareLimit", "200000"));
			dataCompareRowsLimit = Integer.parseInt(properties.getProperty("dataCompareRowsLimit", "2000000"));
			this.startCompareTask();
		}
		context.getMetrics().register(MetricRegistry.name(context.getConfig().metricsPrefix), new BinlogDelayGaugeSet(context));
	}

	@Override
	public void push(RowMap r) throws Exception {
		Deque<UpdateSql> sqlList = this.getSqlList();
		if (sqlList.isEmpty() && r instanceof HeartbeatRowMap) {
			this.setPosition(r);
			return;
		}
		Long now = System.currentTimeMillis();
		if (now - getLastUpdate() > 1000 && sqlList.size() > 0 && sqlList.getLast().getRowMap().isTXCommit()) {
			this.batchUpdate(sqlList);
		}
		if (!syncDbs.contains(r.getDatabase())) {
			return;
		}
		UpdateSql sql = null;
		switch (r.getRowType()) {
			case "insert":
			case "bootstrap-insert":
				sql = this.sqlInsert(r);
				break;
			case "update":
				if (isDoris()) {
					sql = this.sqlInsert(r);
				} else {
					sql = this.sqlUpdate(r);
				}
				break;
			case "delete":
				sql = this.sqlDelete(r);
				break;
			case "table-create":
			case "table-alter":
			case "ddl":
				this.batchUpdate(sqlList);
				if (r.getTable() != null) {
					LOG.info("ddl={}", this.toJSON(r));
					if (!tableSyncLogic.specialDDL(r)) {
						tableSyncLogic.syncTable(r.getDatabase(), r.getTable());
					}
					this.setPosition(r);
				} else {
					LOG.warn("unrecognizable ddl:{}", toJSON(r));
				}
				break;
			case "bootstrap-start":
			case "bootstrap-complete":
				LOG.info("bootstrap:" + this.toJSON(r));
				break;
			case "heartbeat":
				break;
			default:
				this.batchUpdate(sqlList);
				LOG.warn("unrecognizable type:{}", toJSON(r));
				break;
		}
		if (sql != null) {
			if (sqlList.size() >= batchLimit && sqlList.getLast().getRowMap().isTXCommit() || sqlList.size() >= batchTransactionLimit) {
				// need batch commit or reached max batch size
				this.batchUpdate(sqlList);
			}
			this.addSql(sql);
		}
	}

	private void setPosition(RowMap r) {
		if (r.getNextPosition() != null) {
			this.context.setPosition(r);
		} else if (r.getBindObject() instanceof Acknowledgment) {
			((Acknowledgment) r.getBindObject()).acknowledge();
		}
	}

	private Long getLastUpdate() {
		Thread thread = Thread.currentThread();
		Long lastUpdate = lastUpdateMap.get(thread);
		if (lastUpdate == null) {
			lastUpdate = System.currentTimeMillis();
			lastUpdateMap.put(thread, lastUpdate);
		}
		return lastUpdate;
	}

	private Long setLastUpdate(Thread thread) {
		Long lastUpdate = System.currentTimeMillis();
		lastUpdateMap.put(thread, lastUpdate);
		return lastUpdate;
	}

	private Deque<UpdateSql> getSqlList() {
		Thread thread = Thread.currentThread();
		return getSqlList(thread);
	}

	private Deque<UpdateSql> getSqlList(Thread thread) {
		Deque<UpdateSql> sqlList = sqlListMap.get(thread);
		if (sqlList == null) {
			sqlList = new ArrayDeque<>();
			sqlListMap.put(thread, sqlList);
		}
		return sqlList;
	}

	private void addSql(UpdateSql sql) {
		this.getSqlList().add(sql);
	}

	private void batchUpdate(Deque<UpdateSql> sqlList) {
		this.batchUpdate(sqlList, Thread.currentThread());
	}

	private void batchUpdate(Deque<UpdateSql> sqlList, Thread thread) {
		synchronized (thread) {
			if (sqlList == null) {
				sqlList = getSqlList(thread);
			}
			this.doBatchUpdate(sqlList, thread);
		}
	}

	private void doBatchUpdate(Deque<UpdateSql> sqlList, Thread thread) {
		Long lastUpdate = this.setLastUpdate(thread);
		if (sqlList.isEmpty()) {
			return;
		}
		UpdateSql updateSql = sqlList.getLast();
		RowMap rowMap = updateSql.getRowMap();
		Deque<UpdateSqlGroup> groupList = this.groupMergeSql(sqlList);
		TransactionStatus status = isDoris() ? null : targetTransactionManager.getTransaction(new DefaultTransactionDefinition());
		try {
			for (UpdateSqlGroup group : groupList) {
				if (isDoris() && group.getSql().startsWith("insert")) {
					dorisLogic.streamLoad(getSchema(group.getLastRowMap().getDatabase()), group.getLastRowMap().getTable(), group.getDataList());
				} else {
					this.batchUpdateGroup(group);
				}
			}
			if (!isDoris()) {
				targetTransactionManager.commit(status);
			}
			this.setPosition(rowMap);
			LOG.info("batchUpdate size={},mergeSize={},time={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate);
			sqlList.clear();
		} catch (Exception e) {
			LOG.info("batchUpdate fail size={},mergeSize={},time={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate);
			if (!isDoris()) {
				targetTransactionManager.rollback(status);
			}
			if (this.isMsgException(e, "does not exist")) {
				for (UpdateSqlGroup group : groupList) {
					try {
						this.batchUpdateGroup(group);
					} catch (Exception e1) {
						if (this.isMsgException(e1, "does not exist")) {
							if (tableSyncLogic.syncTable(group.getLastRowMap().getDatabase(), group.getLastRowMap().getTable())) {
								this.batchUpdateGroup(group);
							}
						} else {
							throw e1;
						}
					}
				}
				this.setPosition(rowMap);
				sqlList.clear();
			} else if (this.isMsgException(e, "duplicate key value")) {
				Iterator<UpdateSql> it = sqlList.iterator();
				while (it.hasNext()) {
					UpdateSql sql = it.next();
					try {
						this.targetJdbcTemplate.update(sql.getSql(), sql.getArgs());
					} catch (Exception e1) {
						if (!this.isMsgException(e1, "duplicate key value")) {
							throw e1;
						}
						LOG.warn("duplicate key={}", toJSON(sql.getRowMap()));
					}
					this.setPosition(sql.getRowMap());
					it.remove();
				}
			} else {
				throw e;
			}
		}
	}

	private void batchUpdateGroup(UpdateSqlGroup group) {
		long start = System.currentTimeMillis();
		String sql = group.getSqlWithArgsList().size() > group.getArgsList().size() ? (group.getSql() + "...") : group.getSql();
		if (group.getSqlWithArgsList().size() > group.getArgsList().size()) {
			targetJdbcTemplate.batchUpdate(group.getSqlWithArgsList().toArray(new String[group.getSqlWithArgsList().size()]));
			LOG.info("batchUpdateGroup1 size={},time={},sql={}", group.getSqlWithArgsList().size(), System.currentTimeMillis() - start, sql);
		} else {
			this.targetJdbcTemplate.batchUpdate(group.getSql(), group.getArgsList());
			LOG.info("batchUpdateGroup2 size={},time={},sql={}", group.getDataList().size(), System.currentTimeMillis() - start, sql);
		}
	}

	/**
	 * Consecutive and identical sql's are divided into the same group
	 *
	 * @param sqlList
	 * @return
	 */
	private Deque<UpdateSqlGroup> groupMergeSql(Deque<UpdateSql> sqlList) {
		Deque<UpdateSqlGroup> ret = new ArrayDeque<>();
		for (UpdateSql sql : sqlList) {
			UpdateSqlGroup group;
			// PreparedStatement can have at most 65,535 parameters
			if (ret.isEmpty() || !ret.getLast().getSql().equals(sql.getSql()) //
				|| ("delete".equals(ret.getLast().getLastRowMap().getRowType()) && ret.getLast().getArgsList().size() * ret.getLast().getArgsList().get(0).length >= 65000)) {
				group = new UpdateSqlGroup(sql.getSql());
				ret.add(group);
			} else {
				group = ret.getLast();
			}
			group.setLastRowMap(sql.getRowMap());
			group.getArgsList().add(sql.getArgs());
			group.getDataList().add(sql.getRowMap().getData());
			if (sql.getSqlWithArgs() != null) {
				group.getSqlWithArgsList().add(sql.getSqlWithArgs());
			}
			if (group.getArgsList().size() != group.getSqlWithArgsList().size()) {
				group.getSqlWithArgsList().clear();
			}
		}
		for (UpdateSqlGroup group : ret) {
			RowMap r = group.getLastRowMap();
			if (r.getPrimaryKeyColumns().size() == 1 && group.getArgsList().size() > 1) {
				// merge delete sql  "delete from table where id=? ..." to "delete from table where id in (?,?...)
				Object[] updateArgs = group.getArgsList().get(0);
				boolean merge = true;
				if ("update".equals(r.getRowType())) {
					// merge update sql "update table set a=?,b=? where id=? ..." to "update table set a=?,b=? where id in(?,?...)"
					Object[] before = null;
					int argsLen = updateArgs.length - 1;
					for (Object[] args : group.getArgsList()) {
						if (before != null && !Arrays.equals(before, 0, argsLen, args, 0, argsLen)) {
							merge = false;
							break;
						}
						before = args;
					}
				}
				if ("delete".equals(r.getRowType()) || (merge && "update".equals(r.getRowType()))) {
					String key = delimit(r.getPrimaryKeyColumns().get(0));
					int keyLoc = group.getSql().lastIndexOf(key + "=?");
					Object[] ids = group.getArgsList().stream().map(args -> args[args.length - 1]).toArray(size -> new Object[size]);
					String sql = new StringBuilder().append(group.getSql().substring(0, keyLoc)).append(key).append(" in (").append(String.join(",", Collections.nCopies(ids.length, "?"))).append(")").toString();
					group.setSql(sql);
					group.setArgsList(new ArrayList<>());
					if ("update".equals(r.getRowType())) {
						group.getArgsList().add(ArrayUtils.addAll(Arrays.copyOfRange(updateArgs, 0, updateArgs.length - 1), ids));
					} else {
						group.getArgsList().add(ids);
					}
					group.getSqlWithArgsList().clear();
				}
			}
		}
		if (ret.size() > 1 && !isDoris()) {
			Iterator<UpdateSqlGroup> iterator = ret.iterator();
			UpdateSqlGroup last = null;
			while (iterator.hasNext()) {
				UpdateSqlGroup next = iterator.next();
				if (last != null && !last.getSqlWithArgsList().isEmpty() && !next.getSqlWithArgsList().isEmpty() && next.getArgsList().size() < sqlMergeSize) {
					last.getSqlWithArgsList().addAll(next.getSqlWithArgsList());
					last.getDataList().addAll(next.getDataList());
					last.setLastRowMap(next.getLastRowMap());
					iterator.remove();
				} else {
					last = next;
				}
			}
		}
		return ret;
	}

	private boolean isMsgException(Exception e, String msg) {
		Throwable cause = e;
		while (true) {
			if (cause == null) {
				break;
			}
			if (cause.getMessage() != null && cause.getMessage().contains(msg)) {
				return true;
			} else {
				cause = cause.getCause();
			}
		}
		return false;
	}

	private Object convertValue(Object value) {
		if (value instanceof Collection) {
			value = StringUtils.join((Collection) value, ",");
		}
		return value;
	}

	private UpdateSql sqlInsert(RowMap r) {
		List<String> keys = r.getPrimaryKeyColumns();
		if (keys.isEmpty()) {
			return null;
		}
		StringBuilder sql = new StringBuilder();
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlV = new StringBuilder();
		Object[] args = new Object[r.getData().size()];
		int i = 0;
		for (Map.Entry<String, Object> e : r.getData().entrySet()) {
			sqlK.append(delimit(e.getKey())).append(',');
			sqlV.append("?,");
			args[i++] = this.convertValue(e.getValue());
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		sqlV.deleteCharAt(sqlV.length() - 1);
		// insert into %s.%s(%s) values(%s) on conflict(%s) do nothing
		if (!isDoris() && resolvePkConflict) {
			if (isPg()) {
				sql.append("insert into ").append(delimit(getSchema(r.getDatabase()), r.getTable())).append('(').append(sqlK).append(") values(").append(sqlV).append(')') //
					.append(" on conflict(").append(delimit(StringUtils.join(keys, delimit(",")))).append(") do nothing");
			} else {
				sql.append("insert ignore into ").append(delimit(getSchema(r.getDatabase()), r.getTable())).append('(').append(sqlK).append(") values(").append(sqlV).append(')');
			}
		} else {
			sql.append("insert into ").append(delimit(getSchema(r.getDatabase()), r.getTable())).append('(').append(sqlK).append(") values(").append(sqlV).append(')');
		}
		return new UpdateSql(sql.toString(), args, r);
	}

	private UpdateSql sqlUpdate(RowMap r) {
		List<String> keys = r.getPrimaryKeyColumns();
		if (keys.isEmpty()) {
			return null;
		}
		LinkedHashMap<String, Object> data = r.getData();
		LinkedHashMap<String, Object> oldData = r.getOldData();
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlPri = new StringBuilder();
		StringBuilder sqlWithArgs = new StringBuilder();
		Object[] args = new Object[oldData.size() + keys.size()];
		int i = 0;
		for (String updateKey : oldData.keySet()) {
			String key = this.delimit(updateKey);
			Object value = this.convertValue(data.get(updateKey));
			sqlK.append(key).append("=?,");
			args[i++] = value;
			if (value instanceof Number && sqlWithArgs != null) {
				sqlWithArgs.append(key).append('=').append(value).append(',');
			} else {
				sqlWithArgs = null;
			}
		}
		if (sqlK.length() == 0) {
			return null;
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		if (sqlWithArgs != null) {
			sqlWithArgs.deleteCharAt(sqlWithArgs.length() - 1);
			sqlWithArgs.append(" where ");
		}
		for (String pri : keys) {
			String key = this.delimit(pri);
			Object value = oldData.get(pri);
			if (value == null) {
				value = data.get(pri);
			}
			sqlPri.append(key).append("=? and ");
			args[i++] = value;
			if (sqlWithArgs != null && value instanceof Number) {
				sqlWithArgs.append(key).append('=').append(value).append("and ");
			} else {
				sqlWithArgs = null;
			}
		}
		sqlPri.delete(sqlPri.length() - 4, sqlPri.length());
		// update "%s"."%s" set %s where %s
		String table = this.delimit(getSchema(r.getDatabase()), r.getTable());
		String sql = new StringBuilder().append("update ").append(table).append(" set ").append(sqlK).append(" where ").append(sqlPri).toString();
		if (sqlWithArgs != null) {
			sqlWithArgs.delete(sqlWithArgs.length() - 4, sqlWithArgs.length());
			sqlWithArgs.insert(0, "update " + table + " set ");
		}
		return new UpdateSql(sql, args, r, sqlWithArgs == null ? null : sqlWithArgs.toString());
	}

	private UpdateSql sqlDelete(RowMap r) {
		if (r.getPrimaryKeyColumns().isEmpty()) {
			return null;
		}
		LinkedHashMap<String, Object> data = r.getData();
		StringBuilder sqlPri = new StringBuilder();
		Object[] args = new Object[r.getPrimaryKeyColumns().size()];
		int i = 0;
		for (String pri : r.getPrimaryKeyColumns()) {
			sqlPri.append(delimit(pri)).append("=? and ");
			args[i++] = data.get(pri);
		}
		sqlPri.delete(sqlPri.length() - 4, sqlPri.length());
		// delete from "%s"."%s" where %s
		String sql = new StringBuilder().append("delete from ").append(delimit(getSchema(r.getDatabase()), r.getTable())).append(" where ").append(sqlPri).toString();
		return new UpdateSql(sql, args, r);
	}

	public char quote() {
		return isPg() ? '"' : '`';
	}

	public String delimit(String... keys) {
		char quote = quote();
		StringBuilder s = new StringBuilder();
		for (String key : keys) {
			s.append(quote).append(key).append(quote).append('.');
		}
		if (s.length() > 0) {
			s.deleteCharAt(s.length() - 1);
		}
		return s.toString();
	}

	public String toJSON(RowMap r) {
		try {
			return r.toJSON(outputConfig);
		} catch (Exception e) {
			LOG.error("toJSON error:{}", r, e);
		}
		return null;
	}

	public String getSchema(String schema) {
		String value = syncDbMap.get(schema);
		if (value == null || value.trim().isEmpty()) {
			return schema;
		} else {
			return value;
		}
	}

	public String getType() {
		return type;
	}

	public boolean isPg() {
		return "postgresql".equals(type);
	}

	public boolean isMysql() {
		return "mysql".equals(type);
	}

	public boolean isDoris() {
		return "doris".equals(type) || isStarRocks();
	}

	public boolean isStarRocks() {
		return "starrocks".equals(type);
	}

	public JdbcTemplate getTargetJdbcTemplate() {
		return targetJdbcTemplate;
	}

	public JdbcTemplate getMysqlJdbcTemplate() {
		return mysqlJdbcTemplate;
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}


	@Override
	public void requestStop() throws Exception {
		if (targetDs != null) {
			targetDs.close();
		}
		this.flushQueue(0L);
		if (dorisLogic != null) {
			dorisLogic.stop();
		}
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {

	}

	private void startHeartbeatTask() {
		Timer timer = new Timer();
		Long interval = heartbeatSecond * 1000L;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				flushQueue(interval);
			}
		}, interval, interval);
	}

	private void startCompareTask() {
		Timer timer = new Timer();
		Long interval = dataCompareSecond * 1000L;
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				dataCompareLogic.compare(null);
			}
		}, interval, interval);
	}

	private void flushQueue(Long heartbeatInterval) {
		Long now = System.currentTimeMillis();
		for (Map.Entry<Thread, Long> entry : lastUpdateMap.entrySet()) {
			if (now - entry.getValue() > heartbeatInterval) {
				batchUpdate(null, entry.getKey());
			}
		}
	}

	private void startSyncIndexTask(long syncIndexMinute) {
		if (syncIndexMinute <= 0) {
			return;
		}
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				for (String database : syncDbs) {
					LOG.info("SyncIndexTask start:{}", database);
					List<String> tables = tableSyncLogic.getMysqlTables(database);
					for (String table : tables) {
						tableSyncLogic.syncIndex(database, table);
					}
					LOG.info("SyncIndexTask end:{}", database);
				}
			}
		}, 60_000L, syncIndexMinute * 60_000L);
	}

	private void initSchemas(Set<String> syncDbs, boolean initData) {
		if (initData) {
			Connection replicationConnection = null;
			try {
				replicationConnection = mysqlJdbcTemplate.getDataSource().getConnection();
				this.initData(syncDbs, replicationConnection);
			} catch (SQLException e) {
				LOG.error("sql error", e);
				throw new RuntimeException(e);
			} finally {
				try {
					if (replicationConnection != null && !replicationConnection.isClosed()) {
						if (initDataLock) {
							this.executeWithConn(replicationConnection, "unlock tables;");
						}
						DataSourceUtils.releaseConnection(replicationConnection, mysqlJdbcTemplate.getDataSource());
					}
				} catch (SQLException e) {
					LOG.error("close error", e);
				}
			}
		} else {
			for (String database : syncDbs) {
				LOG.info("syncDatabase start:{}", database);
				List<String> tables = tableSyncLogic.getMysqlTables(database);
				for (String table : tables) {
					tableSyncLogic.syncTable(database, table);
				}
				LOG.info("syncDatabase end:{}", database);
			}
		}
	}

	private void executeWithConn(Connection conn, String sql) throws SQLException {
		try (Statement s = conn.createStatement()) {
			s.execute(sql);
		}
	}

	private void targetExecute(String sql) {
		LOG.info("targetExecute:{}", sql);
		targetJdbcTemplate.execute(sql);
	}

	// Refer to  https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/MySQL/MaterializeMetadata.cpp
	private void initData(Set<String> syncDbs, Connection replicationConnection) throws SQLException {
		long start = System.currentTimeMillis();
		// flush tables;
		// flush tables with read lock;
		// show master status;
		if (initDataLock) {
			this.executeWithConn(replicationConnection, "flush tables;");
			this.executeWithConn(replicationConnection, "flush tables with read lock;");
		}
		initPosition = Position.capture(replicationConnection, context.getConfig().gtidMode);
		LOG.info("current position={}", initPosition);
		// set session transaction isolation level repeatable read;
		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		definition.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
		TransactionStatus status = mysqlTransactionManager.getTransaction(definition);
		// not start transaction with consistent snapshot
		boolean transactionStart = false;
		ThreadPoolExecutor executor = new ThreadPoolExecutor(initDataThreadNum, initDataThreadNum, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<>(initDataThreadNum), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
		Integer insertCount = 0;
		try {
			for (String database : syncDbs) {
				List<String> tables = tableSyncLogic.getMysqlTables(database);
				for (String table : tables) {
					if (!transactionStart) {
						mysqlJdbcTemplate.execute(String.format("select 1 from `%s`.`%s` limit 1", database, table));
						if (initDataLock) {
							this.executeWithConn(replicationConnection, "unlock tables;");
						}
						DataSourceUtils.releaseConnection(replicationConnection, mysqlJdbcTemplate.getDataSource());
						LOG.info("lockTableTime={}", System.currentTimeMillis() - start);
						transactionStart = true;
					}
					if (!tableSyncLogic.syncTable(database, table, false)) {
						LOG.warn("primary key not found: {}.{}", database, table);
						continue;
					}
					insertCount += this.initTableData(database, table, executor);
					tableSyncLogic.syncIndex(database, table);
				}
			}
			// commit;
			mysqlTransactionManager.commit(status);
			waitFinish(executor);
		} catch (Exception e) {
			mysqlTransactionManager.rollback(status);
			LOG.error("sync data error", e);
		} finally {
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		LOG.info("insertCount={},time={}", insertCount, System.currentTimeMillis() - start);
	}

	private void waitFinish(ThreadPoolExecutor executor) {
		while (executor.getTaskCount() != executor.getCompletedTaskCount()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private Integer initTableData(String database, String table, ThreadPoolExecutor executor) {
		String querySql = String.format("select * from `%s`.`%s`", database, table);
		Integer count = mysqlJdbcTemplate.query(con -> {
			final PreparedStatement statement = con.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(Integer.MIN_VALUE);
			return statement;
		}, new MyResultSetExtractor(database, table, executor));
		return count;
	}

	public class MyResultSetExtractor implements ResultSetExtractor<Integer> {
		final String database;
		final String table;
		final ThreadPoolExecutor executor;
		// if has data
		int rowCount = 0;
		int columnCount = 0;
		String[] columnNames = null;
		String insertSql = null;
		long start = System.currentTimeMillis();

		public MyResultSetExtractor(String database, String table, ThreadPoolExecutor executor) {
			this.database = database;
			this.table = table;
			this.executor = executor;
		}

		private void asyncBatchInsert(final List<Object[]> argsList) {
			executor.execute(() -> {
				long start = System.currentTimeMillis();
				if (isDoris()) {
					dorisLogic.streamLoad(getSchema(database), table, columnNames, argsList);
				} else {
					this.jdbcBatchInsert(argsList);
				}
				long batchTime = System.currentTimeMillis() - start;
				long totalTime = System.currentTimeMillis() - this.start;
				LOG.info("batch init insert,totalCount={},totalTime={},batchSize={},batchTime={},sql={}", rowCount, totalTime, argsList.size(), batchTime, insertSql);
			});
		}

		private void jdbcBatchInsert(List<Object[]> argsList) {
			TransactionStatus status = isDoris() ? null : targetTransactionManager.getTransaction(new DefaultTransactionDefinition());
			try {
				if (isPg()) {
					targetJdbcTemplate.execute("set local synchronous_commit = off");
				}
				targetJdbcTemplate.batchUpdate(insertSql, argsList);
			} catch (Exception e) {
				if (!isDoris()) {
					targetTransactionManager.rollback(status);
				}
				LOG.error("batchUpdate error,sql={},args={}", insertSql, argsList.get(0), e);
				throw e;
			}
			if (!isDoris()) {
				targetTransactionManager.commit(status);
			}
		}


		@Override
		public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
			LOG.info("batch init query start,table={}.{}", database, table);
			List<Object[]> argsList = new ArrayList<>();
			String database = getSchema(this.database);
			while (rs.next()) {
				if (rowCount++ == 0) {
					ResultSetMetaData rsmd = rs.getMetaData();
					columnCount = rsmd.getColumnCount();
					columnNames = new String[columnCount];
					for (int i = 0; i < columnCount; i++) {
						columnNames[i] = JdbcUtils.lookupColumnName(rsmd, i + 1);
					}
					String names = quote() + StringUtils.join(columnNames, quote() + "," + quote()) + quote();
					insertSql = String.format("insert into %s(%s) values(%s)", delimit(database, table), names, StringUtils.join(Collections.nCopies(columnNames.length, "?"), ","));
					if (targetJdbcTemplate.queryForList(String.format("select 1 from %s limit 1", delimit(database, table)), Integer.class).size() > 0) {
						if (initDataDelete) {
							try {
								targetExecute("truncate table " + delimit(database, table));
							} catch (Exception e) {
								LOG.info("truncate fail,change to delete... {}", e.getMessage());
								targetExecute("delete from " + delimit(database, table) + " where 1=1");
							}
						} else {
							throw new IllegalArgumentException(String.format("init data fail,target table not empty:%s.%s", database, table));
						}
					}
				}
				Object[] args = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					Object value = JdbcUtils.getResultSetValue(rs, i + 1);
					if (value instanceof Boolean) {
						value = (Boolean) value ? 1 : 0;
					} else if (isPg() && value instanceof String) {
						// fix ERROR: invalid byte sequence for encoding "UTF8": 0x00
						value = ((String) value).replaceAll("\u0000", "");
					}
					args[i] = value;
				}
				argsList.add(args);
				if (argsList.size() >= batchLimit || (isPg() && (argsList.size() + 1) * columnCount >= sqlArgsLimit)) {
					this.asyncBatchInsert(argsList);
					argsList = new ArrayList<>();
				}
			}
			if (argsList.size() > 0) {
				this.asyncBatchInsert(argsList);
			}
			LOG.info("batch init query completed,table={}.{},rowCount={},time={}", database, table, rowCount, System.currentTimeMillis() - start);
			return rowCount;
		}
	}
}
