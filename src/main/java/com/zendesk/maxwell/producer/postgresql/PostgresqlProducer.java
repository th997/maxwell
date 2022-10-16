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

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PostgresqlProducer extends AbstractProducer implements StoppableTask {
	static final Logger LOG = LoggerFactory.getLogger(PostgresqlProducer.class);
	private static final String SQL_CREATE = "create table \"%s\".\"%s\" (%s);";
	private static final String SQL_INSERT = "insert into \"%s\".\"%s\"(%s) values(%s);";
	private static final String SQL_UPDATE = "update \"%s\".\"%s\" set %s where %s;";
	private static final String SQL_DELETE = "delete from \"%s\".\"%s\" where %s;";

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
		postgresJdbcTemplate = new JdbcTemplate(postgresDs, true);
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getMaxwellConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.cpds, true);
	}


	@Override
	public void push(RowMap r) throws Exception {
		String output = r.toJSON(outputConfig);
		if (output == null || !r.shouldOutput(outputConfig)) {
			this.context.setPosition(r);
			return;
		}
		//System.out.println(output);
		try {
			this.handSql(r, output);
		} catch (Throwable e) {
			if (!this.handError(e, r, output)) {
				LOG.error("handSql error", e);
			}
		}
		this.context.setPosition(r);
	}

	private boolean handError(Throwable e, RowMap r, String output) {
		if (e == null) {
			return false;
		}
		if (e.getMessage().contains("does not exist")) {
			try {
				if ("true".equals(customProducerProperties.getProperty("syncAllTables"))) {
					List<String> tables = this.getMysqlTables(r.getDatabase());
					for (String table : tables) {
						this.syncTable(r.getDatabase(), table);
					}
				} else {
					this.syncTable(r.getDatabase(), r.getTable());
				}
				this.handSql(r, output);
				return true;
			} catch (Throwable t) {
				LOG.error("handError fail", t);
			}
		}
		return false;
	}

	private synchronized void syncTable(String database, String table) {
		List<TableColumn> mysqlFields = this.getMysqlFields(database, table);
		List<TableColumn> postgresFields = this.getPostgresFields(database, table);
		List<String> commentSqlList = new ArrayList<>();
		if (postgresFields.isEmpty()) {
			if (!this.existsPostgresDb(database)) {
				return;
			}
			StringBuilder fieldsB = new StringBuilder();
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
			}
			String sql = String.format(SQL_CREATE, database, table, fieldsB);
			this.executeDDL(sql);
			if ("true".equals(customProducerProperties.getProperty("initTableData"))) {
				this.initTableData(database, table);
			}
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
				sql = String.format("alter table \"%s\".\"%s\" add primary key (\"%s\");", database, table, cols);
			} else {
				String uniq = e.getValue().get(0).isNonUnique() ? "" : "unique";
				sql = String.format("create %s index concurrently \"%s\" on \"%s\".\"%s\" (\"%s\");", uniq, postgresIndexName, database, table, cols);
			}
			this.executeDDL(sql);
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

	private void handSql(RowMap r, String output) {
		switch (r.getRowType()) {
			case "insert":
			case "bootstrap-insert":
				this.sqlInsert(r, output);
				break;
			case "update":
				this.sqlUpdate(r, output);
				break;
			case "delete":
				this.sqlDelete(r, output);
				break;
			default:
				break;
		}
	}

	private void sqlInsert(RowMap r, String output) {
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
		try {
			postgresJdbcTemplate.update(sql, args);
		} catch (Exception e) {
			if (e.getMessage().contains("duplicate key value")) {
				LOG.warn("duplicate key value:{}", output);
			} else {
				throw e;
			}
		}
	}

	private void sqlUpdate(RowMap r, String output) {
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
		postgresJdbcTemplate.update(sql, args);
	}

	private void sqlDelete(RowMap r, String output) {
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
		postgresJdbcTemplate.update(sql, args);
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
