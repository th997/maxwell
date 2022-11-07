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
import java.util.stream.Collectors;

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

	private List<UpdateSql> sqlList = new ArrayList<>();
	private volatile Long lastUpdate = System.currentTimeMillis();
	private Integer batchLimit = 1000;

	private Set<String> syncDbs;

	private boolean initSchemas;
	private boolean mergeUpdateSql;

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
		initSchemas = "true".equalsIgnoreCase(pgProperties.getProperty("initSchemas"));
		mergeUpdateSql = "true".equalsIgnoreCase(pgProperties.getProperty("mergeUpdateSql"));
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
			if (now - lastUpdate > 1000 && sqlList.size() > 0) {
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
			if (sqlList.size() == 0) {
				sqlList.add(sql);
			} else {
				UpdateSql last = sqlList.get(sqlList.size() - 1);
				if (sql.getSql().equals(last.getSql()) && sqlList.size() < batchLimit) {
					sqlList.add(sql);
				} else {
					this.batchUpdate(sqlList);
					sqlList.add(sql);
				}
			}
		}
	}

	private void batchUpdate(List<UpdateSql> sqlList) {
		lastUpdate = System.currentTimeMillis();
		if (sqlList.isEmpty()) {
			return;
		}
		List<UpdateSql> mergeUpdateList = this.mergeUpdateSql(sqlList);
		UpdateSql updateSql = sqlList.get(sqlList.size() - 1);
		RowMap rowMap = updateSql.getRowMap();
		List<Object[]> argsList = null;
		if (mergeUpdateList != null) {
			argsList = mergeUpdateList.stream().map(UpdateSql::getArgs).collect(Collectors.toList());
		} else {
			argsList = sqlList.stream().map(UpdateSql::getArgs).collect(Collectors.toList());
		}
		TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
		try {
			if (mergeUpdateList != null) {
				postgresJdbcTemplate.update(mergeUpdateList.get(0).getSql(), mergeUpdateList.get(0).getArgs());
				if (mergeUpdateList.size() > 1) {
					postgresJdbcTemplate.batchUpdate(mergeUpdateList.get(1).getSql(), argsList.subList(1, argsList.size()));
				}
			} else {
				postgresJdbcTemplate.batchUpdate(sqlList.get(0).getSql(), argsList);
			}
			transactionManager.commit(status);
			this.context.setPosition(rowMap);
			LOG.info("batchUpdate size={},merge={},time={},sql={}", sqlList.size(), mergeUpdateList != null, System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			sqlList.clear();
		} catch (Exception e) {
			LOG.warn("batchUpdate fail size={},merge={},time={},sql={}", sqlList.size(), mergeUpdateList != null, System.currentTimeMillis() - lastUpdate, updateSql.getSql());
			transactionManager.rollback(status);
			if (this.isNeedSyncTableException(e)) {
				boolean exists = tableSyncLogic.syncTable(rowMap.getDatabase(), rowMap.getTable());
				if (!exists) {
					return;
				}
				Iterator<UpdateSql> it = sqlList.iterator();
				while (it.hasNext()) {
					UpdateSql sql = it.next();
					this.postgresJdbcTemplate.update(sql.getSql(), sql.getArgs());
					this.context.setPosition(sql.getRowMap());
					it.remove();
				}
			} else {
				throw e;
			}
		}
	}

	private boolean isNeedSyncTableException(Exception e) {
		Throwable cause = e;
		while (true) {
			if (cause == null) {
				break;
			}
			if (cause.getMessage() != null && cause.getMessage().contains("does not exist")) {
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
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlPri = new StringBuilder();
		Object[] args = new Object[data.size()];
		int i = 0;
		for (Map.Entry<String, Object> e : data.entrySet()) {
			if (!keys.contains(e.getKey())) {
				sqlK.append("\"" + e.getKey() + "\"=?,");
				args[i++] = e.getValue();
			}
		}
		if (sqlK.length() == 0) {
			return null;
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		for (String pri : keys) {
			sqlPri.append("\"" + pri + "\"=? and ");
			args[i++] = data.get(pri);
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

	/**
	 * merge sql  "delete from table where id=? ..." to "delete from table where id in (?,?...)
	 * merge sql  "update table set xx=? where id=? ..." to "delete from table where id in (?,?...) and insert into table(xx) values(?...),(?...)
	 *
	 * @param sqlList
	 */
	private List<UpdateSql> mergeUpdateSql(List<UpdateSql> sqlList) {
		if (sqlList.size() < 2) {
			return null;
		}
		RowMap r = sqlList.get(sqlList.size() - 1).getRowMap();
		if (!"delete".equals(r.getRowType()) && !"update".equals(r.getRowType()) || r.getPrimaryKeyColumns().size() != 1) {
			return null;
		}
		if (!mergeUpdateSql && "update".equals(r.getRowType())) {
			return null;
		}
		List<UpdateSql> mergeUpdateList = new ArrayList<>(sqlList.size() + 1);
		List<Object> ids = sqlList.stream().map(sql -> sql.getArgs()[sql.getArgs().length - 1]).collect(Collectors.toList());
		String in = String.format("\"%s\" in (%s)", r.getPrimaryKeyColumns().get(0), String.join(",", Collections.nCopies(ids.size(), "?")));
		String sql = String.format(SQL_DELETE, r.getDatabase(), r.getTable(), in);
		UpdateSql delete = new UpdateSql(sql, ids.toArray(new Object[ids.size()]), r);
		mergeUpdateList.add(delete);
		if ("update".equals(r.getRowType())) {
			for (UpdateSql updateSql : sqlList) {
				mergeUpdateList.add(this.sqlInsert(updateSql.getRowMap()));
			}
		}
		return mergeUpdateList;
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
