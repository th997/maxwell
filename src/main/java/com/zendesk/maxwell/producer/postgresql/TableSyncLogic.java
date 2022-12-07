package com.zendesk.maxwell.producer.postgresql;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * table sync logic
 */
public class TableSyncLogic {
	static final Logger LOG = LoggerFactory.getLogger(PostgresqlProducer.class);

	private static final int PG_KEY_LEN = 64;

	private static final String SQL_CREATE = "create table \"%s\".\"%s\" (%s)";

	private static final String SQL_GET_POSTGRES_INDEX = "SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS key_name, a.attname AS column_name, not x.indisunique AS non_unique, x.indisprimary AS pri, pg_get_indexdef(i.oid) AS index_def FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = any(x.indkey) LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind in('r','m','p') AND i.relkind IN('i','i') AND n.nspname=? AND c.relname=? ORDER BY i.relname, array_position(x.indkey, a.attnum)";
	//private static final String SQL_GET_POSTGRES_INDEX = "select indexname key_name,indexdef index_def from pg_catalog.pg_indexes where schemaname=? and tablename=?";
	private static final String SQL_GET_MYSQL_INDEX = "show index from `%s`.`%s`";

	private static final String SQL_GET_POSTGRES_FIELD = "select column_name,udt_name data_type,character_maximum_length str_len,column_default,is_nullable = 'YES' null_able from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_GET_MYSQL_FIELD = "select column_name,column_type,data_type,column_comment,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' auto_increment from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_GET_MYSQL_TABLE = "select table_name from information_schema.tables where table_type != 'VIEW' and table_schema =?";

	private static final String SQL_POSTGRES_COMMENT = "comment on column \"%s\".\"%s\".\"%s\" is '%s'";

	private static final String SQL_GET_POSTGRES_DB = "select count(*) from information_schema.schemata where schema_name =?";
	private static final String SQL_CREATE_POSTGRES_DB = "create schema \"%s\"";

	private static final String SQL_DROP_POSTGRES_TABLE = "drop table if exists \"%s\".\"%s\"";

	private JdbcTemplate postgresJdbcTemplate;
	private JdbcTemplate mysqlJdbcTemplate;

	public TableSyncLogic(JdbcTemplate mysqlJdbcTemplate, JdbcTemplate postgresJdbcTemplate) {
		this.mysqlJdbcTemplate = mysqlJdbcTemplate;
		this.postgresJdbcTemplate = postgresJdbcTemplate;
	}

	public synchronized boolean syncTable(String database, String table) {
		LOG.info("syncTable start:{}.{}", database, table);
		List<TableColumn> mysqlFields = this.getMysqlFields(database, table);
		if (mysqlFields.isEmpty()) {
			this.executeDDL(String.format(SQL_DROP_POSTGRES_TABLE, database, table));
			return false;
		}
		List<TableColumn> postgresFields = this.getPostgresFields(database, table);
		List<String> commentSqlList = new ArrayList<>();
		if (postgresFields.isEmpty()) {
			if (!this.existsPostgresDb(database)) {
				this.executeDDL(String.format(SQL_CREATE_POSTGRES_DB, database));
				LOG.info("database {} not exists,created it!", database);
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
		} else {
			Map<String, TableColumn> mysqlMap = mysqlFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			Map<String, TableColumn> postgresMap = postgresFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			MapDifference<String, TableColumn> diff = Maps.difference(mysqlMap, postgresMap);
			StringBuilder sql = new StringBuilder();
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnRight().entrySet()) {
				sql.append("drop column \"" + e.getKey() + "\",");
			}
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnLeft().entrySet()) {
				e.getValue().setNullAble(true);
				sql.append("add " + e.getValue().toPostgresCol() + ",");
				if (StringUtils.isNotEmpty(e.getValue().getColumnComment())) {
					commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, e.getValue().getColumnName(), StringEscapeUtils.escapeSql(e.getValue().getColumnComment())));
				}
			}
			for (String key : diff.entriesDiffering().keySet()) {
				TableColumn mysql = mysqlMap.get(key);
				TableColumn postgres = postgresMap.get(key);
				if (!mysql.equalsPostgresCol(postgres)) {
					if (!mysql.isSameType(postgres)) {
						sql.append(String.format("alter column \"%s\" type %s,", mysql.getColumnName(), mysql.toColType()));
					}
					if (!mysql.isSameNullAble(postgres) && mysql.isNullAble()) {
						sql.append(String.format("alter column \"%s\" %s not null,", mysql.getColumnName(), mysql.isNullAble() ? "drop" : "set"));
					}
					if (!mysql.isSameDefault(postgres)) {
						String defStr = mysql.toColDefault();
						sql.append(String.format("alter column \"%s\" %s,", mysql.getColumnName(), defStr.isEmpty() ? "drop default" : "set " + defStr));
					}
					if (StringUtils.isNotEmpty(mysql.getColumnComment())) {
						commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, mysql.getColumnName(), StringEscapeUtils.escapeSql(mysql.getColumnComment())));
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
		LOG.info("syncTable end:{}.{}", database, table);
		return true;
	}

	private void syncIndex(String database, String table) {
		Map<String, List<TableIndex>> mysqlGroup = this.getMysqlIndex(database, table);
		Map<String, List<TableIndex>> postgresGroup = this.getPostgresIndex(database, table);
		MapDifference<String, List<TableIndex>> diff = Maps.difference(mysqlGroup, postgresGroup);
		String pgIndexPrefix = table + "_";
		for (List<TableIndex> index : diff.entriesOnlyOnRight().values()) {
			String keyName = index.get(0).getKeyName();
			if (keyName.startsWith(pgIndexPrefix) && !index.get(0).isPri()) {
				this.executeDDL(String.format("drop index if exists \"%s\".\"%s\"", database, keyName));
			}
		}
		for (List<TableIndex> index : diff.entriesOnlyOnLeft().values()) {
			String keyName = index.get(0).getKeyName();
			String postgresIndexName = pgIndexPrefix + keyName; // The index name should be unique in postgres
			if (postgresIndexName.length() > PG_KEY_LEN) {
				postgresIndexName = postgresIndexName.substring(0, PG_KEY_LEN);
			}
			String cols = StringUtils.join(index.stream().map(TableIndex::getColumnName).collect(Collectors.toList()), "\",\"");
			String uniq = index.get(0).isNonUnique() ? "" : "unique";
			String sql;
			if (!index.get(0).isNonUnique() && "PRIMARY".equals(index.get(0).getKeyName())) {
				sql = String.format("alter table \"%s\".\"%s\" add primary key (\"%s\");", database, table, cols);
			} else {
				sql = String.format("create %s index concurrently \"%s\" on \"%s\".\"%s\" (\"%s\");", uniq, postgresIndexName, database, table, cols);
			}
			try {
				this.executeDDL(sql);
			} catch (Exception ex) {
				LOG.warn("syncIndex fail:{}", sql);
			}
		}
	}

	private void executeDDL(String sql) {
		LOG.info("executeDDL:" + sql);
		postgresJdbcTemplate.execute(sql);
	}

	public List<String> getMysqlTables(String tableSchema) {
		return mysqlJdbcTemplate.queryForList(SQL_GET_MYSQL_TABLE, String.class, tableSchema);
	}

	public List<TableColumn> getMysqlFields(String tableSchema, String tableName) {
		List<TableColumn> list = mysqlJdbcTemplate.query(SQL_GET_MYSQL_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public List<TableColumn> getPostgresFields(String tableSchema, String tableName) {
		List<TableColumn> list = postgresJdbcTemplate.query(SQL_GET_POSTGRES_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public Map<String, List<TableIndex>> getMysqlIndex(String tableSchema, String tableName) {
		String sql = String.format(SQL_GET_MYSQL_INDEX, tableSchema, tableName);
		List<TableIndex> list = mysqlJdbcTemplate.query(sql, BeanPropertyRowMapper.newInstance(TableIndex.class));
		return groupByColumn(list);
	}

	public Map<String, List<TableIndex>> getPostgresIndex(String tableSchema, String tableName) {
		List<TableIndex> list = postgresJdbcTemplate.query(SQL_GET_POSTGRES_INDEX, BeanPropertyRowMapper.newInstance(TableIndex.class), tableSchema, tableName);
		return groupByColumn(list);
	}

	public Map<String, List<TableIndex>> groupByColumn(List<TableIndex> indexList) {
		Map<String, List<TableIndex>> map = indexList.stream().collect(Collectors.groupingBy(TableIndex::getKeyName));
		Map<String, List<TableIndex>> ret = new HashMap<>();
		for (List<TableIndex> index : map.values()) {
			String columns = StringUtils.join(index.stream().map(TableIndex::getColumnName).collect(Collectors.toList()), ",");
			columns = columns + "," + index.get(0).isNonUnique();
			ret.put(columns, index);
		}
		return ret;
	}

	public boolean existsPostgresDb(String database) {
		Integer count = postgresJdbcTemplate.queryForObject(SQL_GET_POSTGRES_DB, Integer.class, database);
		return count > 0;
	}

}
