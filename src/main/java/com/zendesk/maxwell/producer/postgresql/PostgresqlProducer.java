package com.zendesk.maxwell.producer.postgresql;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresqlProducer extends AbstractProducer implements StoppableTask {
	static final Logger LOG = LoggerFactory.getLogger(PostgresqlProducer.class);
	private static final String SQL_CREATE = "create table \"%s\".\"%s\" (%s)";
	private static final String SQL_INSERT = "insert into \"%s\".\"%s\"(%s) values(%s)";
	private static final String SQL_UPDATE = "update \"%s\".\"%s\" set %s where %s";
	private static final String SQL_DELETE = "delete from \"%s\".\"%s\" where %s";

	private static final String SQL_GET_POSTGRES_INDEX = "select indexname key_name,indexdef index_def from pg_catalog.pg_indexes where schemaname=? and tablename=?";
	private static final String SQL_GET_MYSQL_INDEX = "show index from %s.%s";

	private static final String SQL_GET_POSTGRES_FIELD = "select column_name,data_type,character_maximum_length str_len,column_default,is_nullable = 'YES' null_able " +
			"from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_GET_MYSQL_FIELD = "select column_name,data_type,column_comment,character_maximum_length str_len,numeric_precision,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' auto_increment " +
			"from information_schema.columns t where t.table_schema =? and table_name =?";

	private static final String SQL_POSTGRES_COMMENT = "comment on column \"%s\".\"%s\".\"%s\" is '%s'";

	private static final String SQL_GET_POSTGRES_DB = "select count(*) from information_schema.schemata where schema_name =?";

	private Properties customProducerProperties;
	private ComboPooledDataSource postgresDs;
	private JdbcTemplate postgresJdbcTemplate;
	private JdbcTemplate mysqlJdbcTemplate;
	private DataSourceTransactionManager transactionManager;

	private List<UpdateSql> sqlList = new ArrayList<>();

	private volatile Long lastUpdate = System.currentTimeMillis();

	private Integer batchLimit = 1000;

	private ReentrantLock lock = new ReentrantLock();

	public PostgresqlProducer(MaxwellContext context) {
		super(context);
		customProducerProperties = context.getConfig().customProducerProperties;
		postgresDs = new ComboPooledDataSource();
		postgresDs.setJdbcUrl(customProducerProperties.getProperty("url"));
		postgresDs.setUser(customProducerProperties.getProperty("user"));
		postgresDs.setPassword(customProducerProperties.getProperty("password"));
		postgresDs.setTestConnectionOnCheckout(true);
		postgresDs.setMinPoolSize(1);
		postgresDs.setMaxPoolSize(5);
		postgresDs.setMaxIdleTime(180);
		postgresDs.setPreferredTestQuery("select 1");
		postgresDs.setAcquireRetryAttempts(Integer.MAX_VALUE);
		postgresDs.setAcquireRetryDelay(3000);
		postgresJdbcTemplate = new JdbcTemplate(postgresDs, true);
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getMaxwellConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.cpds, true);
		transactionManager = new DataSourceTransactionManager(postgresDs);
	}


	@Override
	public void push(RowMap r) throws Exception {
		if (Thread.currentThread().getName().contains("controller")) {
			while (!lock.tryLock()) {
				Thread.sleep(5000);
			}
		} else {
			lock.lock();
		}
		try {
			this.doPush(r);
		} finally {
			lock.unlock();
		}
	}

	private void doPush(RowMap r) throws Exception {
		Long now = System.currentTimeMillis();
		String output = r.toJSON(outputConfig);
		if (output == null || !r.shouldOutput(outputConfig)) {
			this.context.setPosition(r);
			if (now - lastUpdate > 1000 && sqlList.size() > 0) {
				this.batchUpdate(sqlList);
			}
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
		LOG.info("batchUpdate size={},sql={}", sqlList.size(), sqlList.get(0).getSql());
		RowMap rowMap = sqlList.get(sqlList.size() - 1).getRowMap();
		List<Object[]> argsList = sqlList.stream().map(UpdateSql::getArgs).collect(Collectors.toList());
		TransactionStatus status = transactionManager.getTransaction(new DefaultTransactionDefinition());
		try {
			postgresJdbcTemplate.batchUpdate(sqlList.get(0).getSql(), argsList);
			transactionManager.commit(status);
			sqlList.clear();
			this.context.setPosition(rowMap);
		} catch (Exception e) {
			transactionManager.rollback(status);
			if (e.getMessage() != null && e.getMessage().contains("does not exist")) {
				try {
					if ("true".equals(customProducerProperties.getProperty("syncAllTables"))) {
						List<String> tables = this.getMysqlTables(rowMap.getDatabase());
						for (String table : tables) {
							this.syncTable(rowMap.getDatabase(), table);
						}
					} else {
						this.syncTable(rowMap.getDatabase(), rowMap.getTable());
					}
				} catch (Throwable t) {
					LOG.error("handError fail", t);
				}
				Iterator<UpdateSql> it = sqlList.iterator();
				while (it.hasNext()) {
					UpdateSql sql = it.next();
					this.postgresJdbcTemplate.update(sql.getSql(), sql.getArgs());
					it.remove();
					this.context.setPosition(sql.getRowMap());
				}
			} else {
				throw e;
			}
		}
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
		if (r.getPrimaryKeyColumns().isEmpty()) {
			return null;
		}
		LinkedHashMap<String, Object> data = r.getData();
		StringBuilder sqlK = new StringBuilder();
		StringBuilder sqlPri = new StringBuilder();
		Object[] args = new Object[data.size() + r.getPrimaryKeyColumns().size()];
		int i = 0;
		for (Map.Entry<String, Object> e : data.entrySet()) {
			sqlK.append("\"" + e.getKey() + "\"=?,");
			args[i++] = e.getValue();
		}
		sqlK.deleteCharAt(sqlK.length() - 1);
		for (String pri : r.getPrimaryKeyColumns()) {
			sqlPri.append("\"" + pri + "\"=?,");
			args[i++] = data.get(pri);
		}
		sqlPri.deleteCharAt(sqlPri.length() - 1);
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

	///////////// sync table
	private synchronized void syncTable(String database, String table) {
		List<TableColumn> mysqlFields = this.getMysqlFields(database, table);
		List<TableColumn> postgresFields = this.getPostgresFields(database, table);
		List<String> commentSqlList = new ArrayList<>();
		boolean initData = false;
		if (postgresFields.isEmpty()) {
			if (!this.existsPostgresDb(database)) {
				return;
			}
			StringBuilder fieldsB = new StringBuilder();
			List<String> priKey = new ArrayList<>();
			for (int i = 0, size = mysqlFields.size(); i < size; i++) {
				TableColumn column = mysqlFields.get(i);
				if (i == size - 1) {
					fieldsB.append(column.toPostgresCol());
				} else {
					fieldsB.append(column.toPostgresCol() + " ,\r\n");
				}
				if (StringUtils.isNotEmpty(column.getColumnComment())) {
					commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, column.getColumnName(), StringEscapeUtils.escapeSql(column.getColumnComment())));
				}
				if (column.isPri()) {
					priKey.add(column.getColumnName());
				}
			}
			if (!priKey.isEmpty()) {
				fieldsB.append(String.format(" ,\nprimary key (\"%s\")", StringUtils.join(priKey, "\",\"")));
			}
			String sql = String.format(SQL_CREATE, database, table, fieldsB);
			this.executeDDL(sql);
			initData = "true".equals(customProducerProperties.getProperty("initTableData"));
		} else {
			Map<String, TableColumn> mysqlMap = mysqlFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			Map<String, TableColumn> postgresMap = postgresFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			MapDifference<String, TableColumn> diff = Maps.difference(mysqlMap, postgresMap);
			StringBuilder sql = new StringBuilder();
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnRight().entrySet()) {
				sql.append("drop column \"" + e.getKey() + "\",");
			}
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnLeft().entrySet()) {
				sql.append("add " + e.getValue().toPostgresCol() + ",");
				if (StringUtils.isNotEmpty(e.getValue().getColumnComment())) {
					commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, e.getValue().getColumnName(), StringEscapeUtils.escapeSql(e.getValue().getColumnComment())));
				}
			}
			for (Map.Entry<String, TableColumn> e : diff.entriesInCommon().entrySet()) {
				TableColumn mysql = mysqlMap.get(e.getKey());
				TableColumn postgres = postgresMap.get(e.getKey());
				if (!mysql.equalsPostgresCol(postgres)) {
					sql.append("modify " + e.getValue().toPostgresCol() + ",");
					if (StringUtils.isNotEmpty(e.getValue().getColumnComment())) {
						commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, e.getValue().getColumnName(), StringEscapeUtils.escapeSql(e.getValue().getColumnComment())));
					}
				}
			}
			if (sql.length() > 0) {
				sql.deleteCharAt(sql.length() - 1);
				sql.insert(0, String.format("alter table \"%s\".\"%s\" ", database, table));
				this.executeDDL(sql.toString());
			}
		}
		if (!commentSqlList.isEmpty()) {
			postgresJdbcTemplate.batchUpdate(commentSqlList.toArray(new String[commentSqlList.size()]));
		}
		this.syncIndex(database, table);
		if (initData) {
			this.initTableData(database, table);
		}
	}

	private void syncIndex(String database, String table) {
		List<TableIndex> mysqlIndexes = this.getMysqlIndex(database, table);
		List<TableIndex> postgresIndexes = this.getPostgresIndex(database, table);
		if (mysqlIndexes.isEmpty()) {
			return;
		}
		Map<String, List<TableIndex>> mysqlGroup = mysqlIndexes.stream().collect(Collectors.groupingBy(TableIndex::getKeyName));
		Map<String, List<TableIndex>> postgresGroup = postgresIndexes.stream().collect(Collectors.groupingBy(TableIndex::getKeyName));
		for (Map.Entry<String, List<TableIndex>> e : mysqlGroup.entrySet()) {
			String postgresIndexName = table + "_" + e.getKey(); // postgres索引名称要唯一
			if (postgresGroup.containsKey(postgresIndexName)) {
				continue;
			}
			String cols = StringUtils.join(e.getValue().stream().map(TableIndex::getColumnName).collect(Collectors.toList()), "\",\"");
			String sql;
			if (!e.getValue().get(0).isNonUnique() && "PRIMARY".equals(e.getValue().get(0).getKeyName())) {
				// primary key
			} else {
				String uniq = e.getValue().get(0).isNonUnique() ? "" : "unique";
				sql = String.format("create %s index concurrently \"%s\" on \"%s\".\"%s\" (\"%s\");", uniq, postgresIndexName, database, table, cols);
				this.executeDDL(sql);
			}

		}
	}

	private void initTableData(String database, String table) {
		String sqlCount = String.format("select count(*) from `%s`.`%s`", database, table);
		Long rows = mysqlJdbcTemplate.queryForObject(sqlCount, Long.class);
		String sql = "insert into `bootstrap` (database_name, table_name, where_clause, total_rows, client_id, comment) values(?, ?, ?, ?, ?, ?)";
		mysqlJdbcTemplate.update(sql, database, table, null, rows, "maxwell", "postgres");
	}

	private void executeDDL(String sql) {
		LOG.info("executeDDL:" + sql);
		postgresJdbcTemplate.execute(sql);
	}

	public List<String> getMysqlTables(String tableSchema) {
		String sql = "select table_name from information_schema.tables where table_schema =?";
		return mysqlJdbcTemplate.queryForList(sql, String.class, tableSchema);
	}

	public List<TableColumn> getMysqlFields(String tableSchema, String tableName) {
		List<TableColumn> list = mysqlJdbcTemplate.query(SQL_GET_MYSQL_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public List<TableColumn> getPostgresFields(String tableSchema, String tableName) {
		List<TableColumn> list = postgresJdbcTemplate.query(SQL_GET_POSTGRES_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public List<TableIndex> getMysqlIndex(String tableSchema, String tableName) {
		String sql = String.format(SQL_GET_MYSQL_INDEX, tableSchema, tableName);
		List<TableIndex> list = mysqlJdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(TableIndex.class));
		return list;
	}

	public List<TableIndex> getPostgresIndex(String tableSchema, String tableName) {
		List<TableIndex> list = postgresJdbcTemplate.query(SQL_GET_POSTGRES_INDEX, BeanPropertyRowMapper.newInstance(TableIndex.class), tableSchema, tableName);
		return list;
	}

	private boolean existsPostgresDb(String database) {
		Integer count = postgresJdbcTemplate.queryForObject(SQL_GET_POSTGRES_DB, Integer.class, database);
		return count > 0;
	}

}
