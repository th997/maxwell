package com.zendesk.maxwell.producer.postgresql;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedTableAlter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
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

	private static final String SQL_MYSQL_DBS = "select schema_name from information_schema.schemata where schema_name not in ('information_schema','pg_catalog','public')";

	private JdbcTemplate postgresJdbcTemplate;
	private JdbcTemplate mysqlJdbcTemplate;

	public TableSyncLogic(JdbcTemplate mysqlJdbcTemplate, JdbcTemplate postgresJdbcTemplate) {
		this.mysqlJdbcTemplate = mysqlJdbcTemplate;
		this.postgresJdbcTemplate = postgresJdbcTemplate;
	}

	public synchronized boolean syncTable(String database, String table) {
		return syncTable(database, table, true);
	}

	public synchronized boolean syncTable(String database, String table, boolean syncIndex) {
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
			if (sql.length() > 0) {
				sql.deleteCharAt(sql.length() - 1);
				sql.insert(0, String.format("alter table \"%s\".\"%s\" ", database, table));
				this.executeDDL(sql.toString());
			}
			for (String key : diff.entriesDiffering().keySet()) {
				TableColumn mysql = mysqlMap.get(key);
				TableColumn postgres = postgresMap.get(key);
				if (!mysql.equalsPostgresCol(postgres)) {
					if (!mysql.isSameDefault(postgres)) {
						String defStr = mysql.toColDefault();
						this.executeDDL(String.format("alter table \"%s\".\"%s\" alter column \"%s\" %s", database, table, mysql.getColumnName(), defStr.isEmpty() ? "drop default" : "set " + defStr));
					}
					if (!mysql.isSameNullAble(postgres) && mysql.isNullAble()) {
						this.executeDDL(String.format("alter table \"%s\".\"%s\" alter column \"%s\" %s not null", database, table, mysql.getColumnName(), mysql.isNullAble() ? "drop" : "set"));
					}
					if (!mysql.isSameType(postgres)) {
						this.executeDDL(String.format("alter table \"%s\".\"%s\" alter column \"%s\" type %s using \"%s\"::%s", database, table, mysql.getColumnName(), mysql.toColType(), mysql.getColumnName(), mysql.typeGet()));
					}
					if (StringUtils.isNotEmpty(mysql.getColumnComment())) {
						commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, mysql.getColumnName(), StringEscapeUtils.escapeSql(mysql.getColumnComment())));
					}
				}
			}
		}
		if (!commentSqlList.isEmpty()) {
			postgresJdbcTemplate.batchUpdate(commentSqlList.toArray(new String[commentSqlList.size()]));
		}
		if (syncIndex) {
			this.syncIndex(database, table);
		}
		LOG.info("syncTable end:{}.{}", database, table);
		return true;
	}

	public void syncIndex(String database, String table) {
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

	public Collection<String> getDbs() {
		return mysqlJdbcTemplate.queryForList(SQL_MYSQL_DBS, String.class);
	}

	public boolean ddlRename(RowMap r) {
		if (!(r instanceof DDLMap)) {
			return false;
		}
		String sql = ((DDLMap) r).getSql();
		if (sql == null) {
			return false;
		}
		// /* xxx */ alter table xxx
		if (sql.startsWith("/*")) {
			int loc = sql.indexOf("*/");
			if (loc > 0) {
				sql = sql.substring(loc + 2).trim();
			}
		}
		sql = sql.replaceAll("`", "");
		String[] arr = sql.split("\\s+");
		// rename table xx to xx;
		// alter table xx rename to xx;
		if ((arr.length >= 5 && arr[0].equalsIgnoreCase("rename") // 1
			&& arr[1].equalsIgnoreCase("table") // 1
			&& arr[3].equalsIgnoreCase("to") // 1
			&& !arr[2].equalsIgnoreCase(arr[4])) // 1
			|| (arr.length >= 6 && arr[0].equalsIgnoreCase("alter") // 2
			&& arr[1].equalsIgnoreCase("table") // 2
			&& arr[3].equalsIgnoreCase("rename")// 2
			&& arr[4].equalsIgnoreCase("to")// 2
			&& !arr[2].equalsIgnoreCase(arr[5]))) {
			String alterSql = "alter table \"%s\".\"%s\" rename to \"%s\"";
			try {
				if (((DDLMap) r).getChange() instanceof ResolvedTableAlter) {
					ResolvedTableAlter change = (ResolvedTableAlter) ((DDLMap) r).getChange();
					this.executeDDL(String.format(alterSql, change.oldTable.getDatabase(), change.oldTable.getName(), change.newTable.getName()));
					return true;
				}
			} catch (Throwable e) {
				LOG.warn("ddlRename error,sql={}", sql, e);
			}
			return false;
		}
		String oldName = null;
		String newName = null;
		// alter table xxx rename column column_old to column_new
		if (arr.length >= 8 && arr[0].equalsIgnoreCase("alter") //
			&& arr[1].equalsIgnoreCase("table") //
			&& arr[3].equalsIgnoreCase("rename") //
			&& arr[4].equalsIgnoreCase("column") //
			&& arr[6].equalsIgnoreCase("to") //
			&& !arr[5].equalsIgnoreCase(arr[7])) {
			oldName = arr[5];
			newName = arr[7];
		}
		// alter table xxx change column_old column_new xxx
		if (arr.length > 6 && arr[0].equalsIgnoreCase("alter") //
			&& arr[1].equalsIgnoreCase("table") //
			&& arr[3].equalsIgnoreCase("change") //
			&& !arr[4].equalsIgnoreCase(arr[5])) {
			oldName = arr[4];
			newName = arr[5];
		}
		if (oldName != null) {
			String alterSql = "alter table \"%s\".\"%s\" rename column \"%s\" to \"%s\"";
			try {
				this.executeDDL(String.format(alterSql, r.getDatabase(), r.getTable(), oldName, newName));
				return true;
			} catch (Throwable e) {
				LOG.warn("ddlRename error,sql={}", sql, e);
			}
		}
		return false;
	}
}
