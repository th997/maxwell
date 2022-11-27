package com.zendesk.maxwell.producer.postgresql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class PostgresqlProducer extends AbstractProducer implements StoppableTask {
	static final Logger LOG = LoggerFactory.getLogger(PostgresqlProducer.class);

	private static final String SQL_INSERT = "insert into \"%s\".\"%s\"(%s) values(%s)";
	private static final String SQL_UPDATE = "update \"%s\".\"%s\" set %s where %s";
	private static final String SQL_DELETE = "delete from \"%s\".\"%s\" where %s";

	private Properties pgProperties;
	private ComboPooledDataSource postgresDs;
	private JdbcTemplate postgresJdbcTemplate;
	private JdbcTemplate mysqlJdbcTemplate;
	private DataSourceTransactionManager pgTransactionManager;
	private DataSourceTransactionManager mysqlTransactionManager;
	private TableSyncLogic tableSyncLogic;
	private Map<String, String> columnMap = new HashMap<>();

	private LinkedList<UpdateSql> sqlList = new LinkedList<>();
	private volatile Long lastUpdate = System.currentTimeMillis();
	private Integer batchLimit;
	private Integer batchTransactionLimit;

	private Set<String> syncDbs;
	private Set<String> asyncCommitTables;

	private boolean initSchemas;
	private boolean initData;
	private boolean initDataLock;
	private Integer initDataThreadNum;

	private Position initPosition = null;

	public PostgresqlProducer(MaxwellContext context) {
		super(context);
		pgProperties = context.getConfig().pgProperties;
		syncDbs = new HashSet<>(Arrays.asList(StringUtils.split(pgProperties.getProperty("syncDbs", ""), ",")));
		asyncCommitTables = new HashSet<>(Arrays.asList(StringUtils.split(pgProperties.getProperty("asyncCommitTables", ""), ",")));
		initSchemas = "true".equalsIgnoreCase(pgProperties.getProperty("initSchemas"));
		initData = "true".equalsIgnoreCase(pgProperties.getProperty("initData"));
		initDataLock = "true".equalsIgnoreCase(pgProperties.getProperty("initDataLock", "true"));
		initDataThreadNum = Integer.parseInt(pgProperties.getProperty("initDataThreadNum", "10"));
		batchLimit = Integer.parseInt(pgProperties.getProperty("batchLimit", "1000"));
		batchTransactionLimit = Integer.parseInt(pgProperties.getProperty("batchTransactionLimit", "500000"));
		postgresDs = new ComboPooledDataSource();
		postgresDs.setJdbcUrl(pgProperties.getProperty("url"));
		postgresDs.setUser(pgProperties.getProperty("user"));
		postgresDs.setPassword(pgProperties.getProperty("password"));
		postgresDs.setTestConnectionOnCheckout(true);
		postgresDs.setMinPoolSize(1);
		postgresDs.setMaxPoolSize(initDataThreadNum);
		postgresDs.setMaxIdleTime(180);
		postgresDs.setPreferredTestQuery("select 1");
		postgresDs.setAcquireRetryAttempts(Integer.MAX_VALUE);
		postgresDs.setAcquireRetryDelay(3000);
		postgresJdbcTemplate = new JdbcTemplate(postgresDs, true);
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getReplicationConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.getCpds(), true);
		pgTransactionManager = new DataSourceTransactionManager(postgresDs);
		mysqlTransactionManager = new DataSourceTransactionManager(sourcePool.getCpds());
		tableSyncLogic = new TableSyncLogic(mysqlJdbcTemplate, postgresJdbcTemplate);
		if (initSchemas) {
			this.initSchemas(syncDbs, initData);
		}
	}

	@Override
	public void push(RowMap r) throws Exception {
		if (initSchemas) {
			synchronized (this) {
				if (initPosition != null) {
					context.setPosition(initPosition);
				}
				LOG.info("InitSchemas completed!!! The program will exit!!! please set config initSchemas=false and restart,initPosition={}", initPosition);
				System.exit(0);
			}
		}
		this.doPush(r);
	}

	private synchronized void doPush(RowMap r) throws Exception {
		Long now = System.currentTimeMillis();
		String output = r.toJSON(outputConfig);
		if (output == null || !r.shouldOutput(outputConfig)) {
			if (now - lastUpdate > 1000 && sqlList.size() > 0 && sqlList.getLast().getRowMap().isTXCommit()) {
				this.batchUpdate(sqlList);
			}
			return;
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
				sql = this.sqlUpdate(r);
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
					tableSyncLogic.syncTable(r.getDatabase(), r.getTable());
				} else {
					LOG.warn("unrecognizable ddl:{}", toJSON(r));
				}
				break;
			case "bootstrap-start":
			case "bootstrap-complete":
				LOG.info("bootstrap:" + this.toJSON(r));
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

	private void addSql(UpdateSql sql) {
		sqlList.add(sql);
	}

	private void batchUpdate(LinkedList<UpdateSql> sqlList) {
		lastUpdate = System.currentTimeMillis();
		if (sqlList.isEmpty()) {
			return;
		}
		UpdateSql updateSql = sqlList.getLast();
		RowMap rowMap = updateSql.getRowMap();
		List<UpdateSqlGroup> groupList = this.groupMergeSql(sqlList);
		TransactionStatus status = pgTransactionManager.getTransaction(new DefaultTransactionDefinition());
		try {
			if (groupList.size() == 1 && asyncCommitTables.contains(rowMap.getTable())) {
				postgresJdbcTemplate.execute("set local synchronous_commit = off");
			}
			for (UpdateSqlGroup group : groupList) {
				this.postgresJdbcTemplate.batchUpdate(group.getSql(), group.getArgsList());
			}
			pgTransactionManager.commit(status);
			this.context.setPosition(rowMap);
			LOG.info("batchUpdate size={},mergeSize={},time={},sql={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			sqlList.clear();
		} catch (Exception e) {
			LOG.info("batchUpdate fail size={},mergeSize={},time={},sql={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			pgTransactionManager.rollback(status);
			if (this.isMsgException(e, "does not exist")) {
				for (UpdateSqlGroup group : groupList) {
					try {
						this.postgresJdbcTemplate.batchUpdate(group.getSql(), group.getArgsList());
					} catch (Exception e1) {
						if (this.isMsgException(e1, "does not exist")) {
							if (tableSyncLogic.syncTable(rowMap.getDatabase(), rowMap.getTable())) {
								this.postgresJdbcTemplate.batchUpdate(group.getSql(), group.getArgsList());
							}
						} else {
							throw e1;
						}
					}
				}
				this.context.setPosition(rowMap);
				sqlList.clear();
			} else if (this.isMsgException(e, "duplicate key value")) {
				Iterator<UpdateSql> it = sqlList.iterator();
				while (it.hasNext()) {
					UpdateSql sql = it.next();
					try {
						this.postgresJdbcTemplate.update(sql.getSql(), sql.getArgs());
					} catch (Exception e1) {
						if (!this.isMsgException(e1, "duplicate key value")) {
							throw e1;
						}
						LOG.warn("duplicate key={}", toJSON(sql.getRowMap()));
					}
					this.context.setPosition(sql.getRowMap());
					it.remove();
				}
			} else {
				throw e;
			}
		}
	}

	/**
	 * Consecutive and identical sql's are divided into the same group
	 *
	 * @param sqlList
	 * @return
	 */
	private LinkedList<UpdateSqlGroup> groupMergeSql(LinkedList<UpdateSql> sqlList) {
		LinkedList<UpdateSqlGroup> ret = new LinkedList<>();
		for (UpdateSql sql : sqlList) {
			UpdateSqlGroup group;
			// PreparedStatement can have at most 65,535 parameters
			if (ret.isEmpty() || !ret.getLast().getSql().equals(sql.getSql())
					|| ("delete".equals(ret.getLast().getLastRowMap().getRowType()) && ret.getLast().getArgsList().size() * ret.getLast().getArgsList().get(0).length >= 65000)) {
				group = new UpdateSqlGroup(sql.getSql());
				ret.add(group);
			} else {
				group = ret.getLast();
			}
			group.setLastRowMap(sql.getRowMap());
			group.getArgsList().add(sql.getArgs());
		}
		for (UpdateSqlGroup group : ret) {
			RowMap r = group.getLastRowMap();
			if ("delete".equals(r.getRowType()) && r.getPrimaryKeyColumns().size() == 1 && group.getArgsList().size() > 1) {
				// merge delete sql  "delete from table where id=? ..." to "delete from table where id in (?,?...)
				Object[] ids = group.getArgsList().stream().map(args -> args[args.length - 1]).toArray(size -> new Object[size]);
				String in = String.format("\"%s\" in (%s)", r.getPrimaryKeyColumns().get(0), String.join(",", Collections.nCopies(ids.length, "?")));
				String sql = String.format(SQL_DELETE, r.getDatabase(), r.getTable(), in);
				List<Object[]> argsList = new ArrayList<>();
				argsList.add(ids);
				group.setSql(sql);
				group.setArgsList(argsList);
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

	private Object convertValue(RowMap r, Map.Entry<String, Object> e) {
		Object value = e.getValue();
		if (value instanceof Collection) {
			value = StringUtils.join((Collection) value, ",");
		}
		if (value instanceof String) {
			String key = String.format("%s.%s.%s", r.getDatabase(), r.getTable(), e.getKey());
			String type = columnMap.get(key);
			if (type == null) {
				synchronized (columnMap) {
					type = columnMap.get(key);
					if (type == null) {
						try {
							LOG.info("init columnMap,{}", toJSON(r));
							Database database = context.getReplicator().getSchema().findDatabase(r.getDatabase());
							for (Table table : database.getTableList()) {
								for (ColumnDef col : table.getColumnList()) {
									columnMap.put(String.format("%s.%s.%s", database.getName(), table.getName(), col.getName()), col.getType());
								}
							}
						} catch (Throwable t) {
							LOG.error("columnMap get error", t);
						}
					}
				}
			}
			if (type != null && (type.endsWith("blob") || type.endsWith("binary"))) {
				try {
					value = Base64.decodeBase64((String) value);
				} catch (Throwable t) {
					LOG.warn("decodeBase64 error", t);
				}
			}
		}
		return value;
	}

	private UpdateSql sqlInsert(RowMap r) {
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlV = new StringBuilder();
		Object[] args = new Object[r.getData().size()];
		int i = 0;
		for (Map.Entry<String, Object> e : r.getData().entrySet()) {
			sqlK.append("\"" + e.getKey() + "\",");
			sqlV.append("?,");
			args[i++] = this.convertValue(r, e);
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		sqlV.deleteCharAt(sqlV.length() - 1);
		String sql = String.format(SQL_INSERT, r.getDatabase(), r.getTable(), sqlK, sqlV);
		if (!r.getPrimaryKeyColumns().isEmpty()) {
			String extra = String.format(" on conflict(\"%s\") do nothing", StringUtils.join(r.getPrimaryKeyColumns(), "\",\""));
			sql = sql + extra;
		}
		return new UpdateSql(sql, args, r);
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
		Object[] args = new Object[data.size() + keys.size()];
		int i = 0;
		for (Map.Entry<String, Object> e : data.entrySet()) {
			sqlK.append("\"" + e.getKey() + "\"=?,");
			args[i++] = this.convertValue(r, e);
		}
		if (sqlK.length() == 0) {
			return null;
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		for (String pri : keys) {
			sqlPri.append("\"" + pri + "\"=? and ");
			Object priValue = oldData.get(pri);
			if (priValue == null) {
				priValue = data.get(pri);
			}
			args[i++] = priValue;
		}
		sqlPri.delete(sqlPri.length() - 4, sqlPri.length());
		String sql = String.format(SQL_UPDATE, r.getDatabase(), r.getTable(), sqlK, sqlPri);
		return new UpdateSql(sql, args, r);
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
			sqlPri.append("\"" + pri + "\"=? and ");
			args[i++] = data.get(pri);
		}
		sqlPri.delete(sqlPri.length() - 4, sqlPri.length());
		String sql = String.format(SQL_DELETE, r.getDatabase(), r.getTable(), sqlPri);
		return new UpdateSql(sql, args, r);
	}

	public String toJSON(RowMap r) {
		try {
			return r.toJSON(outputConfig);
		} catch (Exception e) {
			LOG.error("toJSON error:{}", r, e);
		}
		return null;
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}


	@Override
	public void requestStop() throws Exception {
		if (postgresDs != null) {
			postgresDs.close();
		}
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {

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
						JdbcUtils.closeConnection(replicationConnection);
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
						JdbcUtils.closeConnection(replicationConnection);
						LOG.info("lockTableTime={}", System.currentTimeMillis() - start);
						transactionStart = true;
					}
					tableSyncLogic.syncTable(database, table);
					insertCount += this.initTableData(database, table, executor);
				}
			}
			// commit;
			mysqlTransactionManager.commit(status);
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

	private Integer initTableData(String database, String table, ThreadPoolExecutor executor) {
		String querySql = String.format("select * from `%s`.`%s`", database, table);
		Integer count = mysqlJdbcTemplate.query(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				final PreparedStatement statement = con.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				statement.setFetchSize(Integer.MIN_VALUE);
				return statement;
			}
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

		private void asyncBatchInsert(final String sql, final List<Object[]> argsList) {
			executor.execute(() -> {
				long start = System.currentTimeMillis();
				TransactionStatus statusPg = pgTransactionManager.getTransaction(new DefaultTransactionDefinition());
				try {
					postgresJdbcTemplate.execute("set local synchronous_commit = off");
					postgresJdbcTemplate.batchUpdate(sql, argsList);
				} catch (Exception e) {
					pgTransactionManager.rollback(statusPg);
					LOG.error("batchUpdate error,sql={},args={}", sql, argsList.get(0), e);
					throw e;
				}
				pgTransactionManager.commit(statusPg);
				LOG.info("batch init insert,size={},time={},sql={}", argsList.size(), System.currentTimeMillis() - start, sql);
			});
		}

		@Override
		public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
			List<Object[]> argsList = new ArrayList<>();
			while (rs.next()) {
				if (rowCount++ == 0) {
					ResultSetMetaData rsmd = rs.getMetaData();
					columnCount = rsmd.getColumnCount();
					columnNames = new String[columnCount];
					for (int i = 0; i < columnCount; i++) {
						columnNames[i] = String.format("\"%s\"", JdbcUtils.lookupColumnName(rsmd, i + 1));
					}
					insertSql = String.format("insert into \"%s\".\"%s\"(%s) values(%s)", database, table, StringUtils.join(columnNames, ","), StringUtils.join(Collections.nCopies(columnNames.length, "?"), ","));
					if (postgresJdbcTemplate.queryForList(String.format("select 1 from \"%s\".\"%s\" limit 1", database, table), Integer.class).size() > 0) {
						throw new IllegalArgumentException(String.format("init data fail,postgresql table not empty:%s.%s", database, table));
					}
				}
				Object[] args = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					Object value = JdbcUtils.getResultSetValue(rs, i + 1);
					if (value instanceof Boolean) {
						value = (Boolean) value ? 1 : 0;
					} else if (value instanceof String) {
						// fix ERROR: invalid byte sequence for encoding "UTF8": 0x00
						value = ((String) value).replaceAll("\u0000", "");
					}
					args[i] = value;
				}
				argsList.add(args);
				if (argsList.size() >= batchLimit) {
					this.asyncBatchInsert(insertSql, argsList);
					argsList = new ArrayList<>();
				}
			}
			if (argsList.size() > 0) {
				this.asyncBatchInsert(insertSql, argsList);
			}
			LOG.info("batch init query completed,table={}.{},rowCount={},time={}", database, table, rowCount, System.currentTimeMillis() - start);
			return rowCount;
		}
	}
}
