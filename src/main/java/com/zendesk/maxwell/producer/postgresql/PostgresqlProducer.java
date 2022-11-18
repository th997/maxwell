package com.zendesk.maxwell.producer.postgresql;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.*;
import java.util.concurrent.TimeoutException;

public class PostgresqlProducer extends AbstractProducer implements StoppableTask {
	static final Logger LOG = LoggerFactory.getLogger(PostgresqlProducer.class);

	private static final String SQL_INSERT = "insert into \"%s\".\"%s\"(%s) values(%s)";
	private static final String SQL_UPDATE = "update \"%s\".\"%s\" set %s where %s";
	private static final String SQL_DELETE = "delete from \"%s\".\"%s\" where %s";

	private Properties pgProperties;
	private ComboPooledDataSource postgresDs;
	private JdbcTemplate postgresJdbcTemplate;
	private JdbcTemplate mysqlJdbcTemplate;
	private DataSourceTransactionManager transactionManager;
	private TableSyncLogic tableSyncLogic;

	private LinkedList<UpdateSql> sqlList = new LinkedList<>();
	private volatile Long lastUpdate = System.currentTimeMillis();
	private Integer batchLimit;
	private Integer batchTransactionLimit;

	private Set<String> syncDbs;
	private Set<String> asyncCommitTables;

	private boolean initSchemas;

	public PostgresqlProducer(MaxwellContext context) {
		super(context);
		pgProperties = context.getConfig().pgProperties;
		postgresDs = new ComboPooledDataSource();
		postgresDs.setJdbcUrl(pgProperties.getProperty("url"));
		postgresDs.setUser(pgProperties.getProperty("user"));
		postgresDs.setPassword(pgProperties.getProperty("password"));
		postgresDs.setTestConnectionOnCheckout(true);
		postgresDs.setMinPoolSize(1);
		postgresDs.setMaxPoolSize(5);
		postgresDs.setMaxIdleTime(180);
		postgresDs.setPreferredTestQuery("select 1");
		postgresDs.setAcquireRetryAttempts(Integer.MAX_VALUE);
		postgresDs.setAcquireRetryDelay(3000);
		postgresJdbcTemplate = new JdbcTemplate(postgresDs, true);
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getMaxwellConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.getCpds(), true);
		transactionManager = new DataSourceTransactionManager(postgresDs);
		tableSyncLogic = new TableSyncLogic(mysqlJdbcTemplate, postgresJdbcTemplate);
		syncDbs = new HashSet<>(Arrays.asList(StringUtils.split(pgProperties.getProperty("syncDbs", ""), ",")));
		asyncCommitTables = new HashSet<>(Arrays.asList(StringUtils.split(pgProperties.getProperty("asyncCommitTables", ""), ",")));
		initSchemas = "true".equalsIgnoreCase(pgProperties.getProperty("initSchemas"));
		batchLimit = Integer.parseInt(pgProperties.getProperty("batchLimit", "1000"));
		batchTransactionLimit = Integer.parseInt(pgProperties.getProperty("batchTransactionLimit", "500000"));
	}

	@Override
	public void push(RowMap r) throws Exception {
		if (initSchemas) {
			synchronized (this) {
				for (String db : syncDbs) {
					tableSyncLogic.syncAllTables(db);
				}
				LOG.info("initSchemas finish, exit...");
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
		TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
		try {
			if (groupList.size() == 1 && asyncCommitTables.contains(rowMap.getTable())) {
				postgresJdbcTemplate.execute("set local synchronous_commit = off");
			}
			for (UpdateSqlGroup group : groupList) {
				this.postgresJdbcTemplate.batchUpdate(group.getSql(), group.getArgsList());
			}
			transactionManager.commit(status);
			this.context.setPosition(rowMap);
			LOG.info("batchUpdate size={},mergeSize={},time={},sql={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			sqlList.clear();
		} catch (Exception e) {
			LOG.info("batchUpdate fail size={},mergeSize={},time={},sql={}", sqlList.size(), groupList.size(), System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			transactionManager.rollback(status);
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
			if (ret.isEmpty() || !ret.getLast().getSql().equals(sql.getSql())) {
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

	private UpdateSql sqlInsert(RowMap r) {
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlV = new StringBuilder();
		Object[] args = new Object[r.getData().size()];
		int i = 0;
		for (Map.Entry<String, Object> e : r.getData().entrySet()) {
			sqlK.append("\"" + e.getKey() + "\",");
			sqlV.append("?,");
			args[i++] = e.getValue();
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
			args[i++] = e.getValue();
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
}
