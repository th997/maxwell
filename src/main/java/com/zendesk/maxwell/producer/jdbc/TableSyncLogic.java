package com.zendesk.maxwell.producer.jdbc;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.zendesk.maxwell.producer.jdbc.converter.Converter;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.*;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * table sync logic
 */
public class TableSyncLogic {
	static final Logger LOG = LoggerFactory.getLogger(TableSyncLogic.class);

	private static final int PG_KEY_LEN = 64;

	private static final String SQL_CREATE = "create table %s (%s)";

	private static final String SQL_GET_POSTGRES_INDEX = "SELECT n.nspname AS schema_name, c.relname AS table_name, i.relname AS key_name, a.attname AS column_name, not x.indisunique AS non_unique, x.indisprimary AS pri, pg_get_indexdef(i.oid) AS index_def FROM pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = any(x.indkey) LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind in('r','m','p') AND i.relkind IN('i','i') AND n.nspname=? AND c.relname=? ORDER BY i.relname, array_position(x.indkey, a.attnum)";
	//private static final String SQL_GET_POSTGRES_INDEX = "select indexname key_name,indexdef index_def from pg_catalog.pg_indexes where schemaname=? and tablename=?";
	private static final String SQL_GET_MYSQL_INDEX = "show index from `%s`.`%s`";

	private static final String SQL_GET_POSTGRES_FIELD = "select column_name,udt_name data_type,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_GET_MYSQL_FIELD = "select column_name,column_type,data_type,column_comment,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' auto_increment from information_schema.columns t where t.table_schema =? and table_name =? order by ordinal_position";
	private static final String SQL_GET_MYSQL_TABLE = "select table_name from information_schema.tables where table_type != 'VIEW' and table_schema =?";
	private static final String SQL_POSTGRES_COMMENT = "comment on column \"%s\".\"%s\".\"%s\" is '%s'";
	private static final String SQL_GET_DB = "select count(*) from information_schema.schemata where schema_name =?";
	private static final String SQL_CREATE_DB = "create schema %s";
	private static final String SQL_DROP_TABLE = "drop table if exists %s";
	private static final String SQL_MYSQL_DBS = "select schema_name from information_schema.schemata where schema_name not in ('mysql','sys','information_schema','performance_schema','pg_catalog','public')";

	private JdbcProducer producer;
	private ExecutorService executorService = Executors.newSingleThreadExecutor();

	public TableSyncLogic(JdbcProducer producer) {
		this.producer = producer;
	}

	public synchronized boolean syncTable(String database, String table) {
		return syncTable(database, table, true);
	}

	public synchronized boolean syncTable(String database, String table, boolean syncIndex) {
		LOG.info("syncTable start:{}.{}", database, table);
		List<TableColumn> mysqlFields = this.getMysqlFields(database, table);
		String fullTableName = producer.delimit(database, table);
		if (mysqlFields.isEmpty()) {
			this.executeDDL(String.format(SQL_DROP_TABLE, fullTableName));
			return false;
		}
		List<TableColumn> targetFields = this.getTargetFields(database, table);
		List<String> commentSqlList = new ArrayList<>();
		if (targetFields.isEmpty()) {
			if (!this.existsTargetDb(database)) {
				this.executeDDL(String.format(SQL_CREATE_DB, producer.delimit(database)));
				LOG.info("database {} not exists,created it!", database);
			}
			StringBuilder fieldsB = new StringBuilder();
			for (int i = 0, size = mysqlFields.size(); i < size; i++) {
				TableColumn column = mysqlFields.get(i);
				Converter converter = Converter.getConverter(column, producer.getType());
				if (i == size - 1) {
					fieldsB.append(converter.toTargetCol());
				} else {
					fieldsB.append(converter.toTargetCol() + " ,\r\n");
				}
				if (producer.isPg() && StringUtils.isNotEmpty(column.getColumnComment())) {
					commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, column.getColumnName(), StringEscapeUtils.escapeSql(column.getColumnComment())));
				}
			}
			String sql = String.format(SQL_CREATE, fullTableName, fieldsB);
			this.executeDDL(sql);
		} else {
			Map<String, TableColumn> mysqlMap = mysqlFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			Map<String, TableColumn> postgresMap = targetFields.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
			MapDifference<String, TableColumn> diff = Maps.difference(mysqlMap, postgresMap);
			StringBuilder sql = new StringBuilder();
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnRight().entrySet()) {
				sql.append("drop column " + producer.delimit(e.getKey()) + ",");
			}
			for (Map.Entry<String, TableColumn> e : diff.entriesOnlyOnLeft().entrySet()) {
				Converter converter = Converter.getConverter(e.getValue(), producer.getType());
				e.getValue().setNullAble(true);
				sql.append("add " + converter.toTargetCol() + ",");
				if (producer.isPg() && StringUtils.isNotEmpty(e.getValue().getColumnComment())) {
					commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, e.getValue().getColumnName(), StringEscapeUtils.escapeSql(e.getValue().getColumnComment())));
				}
			}
			if (sql.length() > 0) {
				sql.deleteCharAt(sql.length() - 1);
				sql.insert(0, String.format("alter table %s ", fullTableName));
				this.executeDDL(sql.toString());
			}
			for (String key : diff.entriesDiffering().keySet()) {
				TableColumn mysql = mysqlMap.get(key);
				TableColumn target = postgresMap.get(key);
				Converter converter = Converter.getConverter(mysql, producer.getType());
				if (!converter.isEqualsTargetCol(target)) {
					String columnName = producer.delimit(mysql.getColumnName());
					if (producer.isPg()) {
						if (!converter.isSameDefault(target)) {
							String defStr = converter.toColDefault();
							this.executeDDL(String.format("alter table %s alter column %s %s", fullTableName, columnName, defStr.isEmpty() ? "drop default" : "set " + defStr));
						}
						if (!converter.isSameNullAble(target) && mysql.isNullAble()) {
							this.executeDDL(String.format("alter table %s alter column %s %s not null", fullTableName, columnName, mysql.isNullAble() ? "drop" : "set"));
						}
						if (!converter.isSameType(target)) {
							this.executeDDL(String.format("alter table %s alter column %s type %s using %s::%s", fullTableName, columnName, converter.toColType(), columnName, converter.typeGet()));
						}
						if (StringUtils.isNotEmpty(mysql.getColumnComment())) {
							commentSqlList.add(String.format(SQL_POSTGRES_COMMENT, database, table, mysql.getColumnName(), StringEscapeUtils.escapeSql(mysql.getColumnComment())));
						}
					} else {
						this.executeDDL(String.format("alter table %s change %s %s", fullTableName, columnName, converter.toTargetCol()));
					}
				}
			}
		}
		if (!commentSqlList.isEmpty()) {
			producer.getTargetJdbcTemplate().batchUpdate(commentSqlList.toArray(new String[commentSqlList.size()]));
		}
		if (syncIndex) {
			if (targetFields.isEmpty()) {
				this.syncIndex(database, table);
			} else {
				executorService.submit(() -> syncIndex(database, table));
			}
		}
		LOG.info("syncTable end:{}.{}", database, table);
		return true;
	}

	public void syncIndex(String database, String table) {
		Map<String, List<TableIndex>> mysqlGroup = this.getMysqlIndex(database, table);
		Map<String, List<TableIndex>> targetGroup = this.getTargetIndex(database, table);
		Iterator<Map.Entry<String, List<TableIndex>>> it = targetGroup.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, List<TableIndex>> e = it.next();
			TableIndex index0 = e.getValue().get(0);
			if (!index0.isNonUnique() && !index0.isPri()) {
				this.dropIndex(database, table, index0);
				it.remove();
			}
		}
		MapDifference<String, List<TableIndex>> diff = Maps.difference(mysqlGroup, targetGroup);
		String pgIndexPrefix = table + "_";
		for (List<TableIndex> index : diff.entriesOnlyOnRight().values()) {
			String keyName = index.get(0).getKeyName();
			if (keyName.startsWith(pgIndexPrefix) && !index.get(0).isPri()) {
				this.dropIndex(database, table, index.get(0));
			}
		}
		for (List<TableIndex> index : diff.entriesOnlyOnLeft().values()) {
			String keyName = index.get(0).getKeyName();
			String indexName = pgIndexPrefix + keyName; // The index name should be unique in postgres
			if (indexName.length() > PG_KEY_LEN) {
				indexName = indexName.substring(0, PG_KEY_LEN);
			}
			String cols = producer.quote() + StringUtils.join(index.stream().map(TableIndex::getColumnName).collect(Collectors.toList()), producer.quote() + "," + producer.quote()) + producer.quote();
			//String uniq = index.get(0).isNonUnique() ? "" : "unique";
			String uniq = "";
			String sql;
			if (!index.get(0).isNonUnique() && "PRIMARY".equals(index.get(0).getKeyName())) {
				sql = String.format("alter table %s add primary key (%s);", producer.delimit(database, table), cols);
			} else {
				sql = String.format("create %s index %s on %s (%s);", uniq, producer.delimit(indexName), producer.isPg() ? "concurrently" : "" + producer.delimit(database, table), cols);
			}
			try {
				this.executeDDL(sql);
			} catch (Exception ex) {
				LOG.warn("syncIndex fail:{}", sql);
			}
		}
	}

	private void dropIndex(String database, String table, TableIndex index0) {
		if (producer.isPg()) {
			this.executeDDL(String.format("drop index if exists %s", producer.delimit(database, index0.getKeyName())));
		} else {
			this.executeDDL(String.format("alter table %s drop index %s", producer.delimit(database, table), producer.delimit(index0.getKeyName())));
		}
	}

	private void executeDDL(String sql) {
		LOG.info("executeDDL:" + sql);
		producer.getTargetJdbcTemplate().execute(sql);
	}

	public List<String> getMysqlTables(String tableSchema) {
		return producer.getMysqlJdbcTemplate().queryForList(SQL_GET_MYSQL_TABLE, String.class, tableSchema);
	}

	public List<TableColumn> getMysqlFields(String tableSchema, String tableName) {
		List<TableColumn> list = producer.getMysqlJdbcTemplate().query(SQL_GET_MYSQL_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public List<TableColumn> getTargetFields(String tableSchema, String tableName) {
		String sql = SQL_GET_MYSQL_FIELD;
		if (producer.isPg()) {
			sql = SQL_GET_POSTGRES_FIELD;
		}
		List<TableColumn> list = producer.getTargetJdbcTemplate().query(sql, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		return list;
	}

	public Map<String, List<TableIndex>> getMysqlIndex(String tableSchema, String tableName) {
		String sql = String.format(SQL_GET_MYSQL_INDEX, tableSchema, tableName);
		List<TableIndex> list = producer.getMysqlJdbcTemplate().query(sql, BeanPropertyRowMapper.newInstance(TableIndex.class));
		return groupByColumn(list);
	}

	public Map<String, List<TableIndex>> getTargetIndex(String tableSchema, String tableName) {
		List<TableIndex> list;
		if (producer.isPg()) {
			list = producer.getTargetJdbcTemplate().query(SQL_GET_POSTGRES_INDEX, BeanPropertyRowMapper.newInstance(TableIndex.class), tableSchema, tableName);
		} else {
			list = producer.getTargetJdbcTemplate().query(String.format(SQL_GET_MYSQL_INDEX, tableSchema, tableName), BeanPropertyRowMapper.newInstance(TableIndex.class));
		}
		return groupByColumn(list);
	}

	public Map<String, List<TableIndex>> groupByColumn(List<TableIndex> indexList) {
		Map<String, List<TableIndex>> map = indexList.stream().collect(Collectors.groupingBy(TableIndex::getKeyName));
		Map<String, List<TableIndex>> ret = new HashMap<>();
		for (List<TableIndex> index : map.values()) {
			String columns = StringUtils.join(index.stream().map(TableIndex::getColumnName).collect(Collectors.toList()), ",");
			// columns = columns + "," + index.get(0).isNonUnique();
			ret.put(columns, index);
		}
		return ret;
	}

	public boolean existsTargetDb(String database) {
		Integer count = producer.getTargetJdbcTemplate().queryForObject(SQL_GET_DB, Integer.class, database);
		return count > 0;
	}

	public Collection<String> getDbs() {
		return producer.getMysqlJdbcTemplate().queryForList(SQL_MYSQL_DBS, String.class);
	}

	public boolean specialDDL(RowMap ddl) {
		if (!(ddl instanceof DDLMap)) {
			return false;
		}
		DDLMap r = (DDLMap) ddl;
		// truncate
		if (r.getChange() instanceof ResolvedTableTruncate) {
			try {
				this.executeDDL(String.format("truncate %s", producer.delimit(r.getDatabase(), r.getTable())));
			} catch (Throwable e) {
				LOG.error("truncate error,sql={}", r.getSql(), e);
			}
			return true;
		}
		if (!(r.getChange() instanceof ResolvedTableAlter)) {
			return false;
		}
		ResolvedTableAlter change = (ResolvedTableAlter) r.getChange();
		// rename table xx to xx;
		// alter table xx rename to xx;
		if (change.oldTable != null && change.newTable != null && !change.oldTable.name.equals(change.newTable.name)) {
			String alterSql = "alter table if exists %s rename to %s";
			try {
				this.executeDDL(String.format(alterSql, producer.delimit(change.oldTable.getDatabase(), change.oldTable.getName()), producer.delimit(change.newTable.getName())));
			} catch (Throwable e) {
				LOG.error("tableRename error,sql={}", r.getSql(), e);
			}
			return true;
		}
		// alter table xxx rename column column_old to column_new
		// alter table xxx change column_old column_new xxx
		if (!CollectionUtils.isEmpty(change.columnMods)) {
			String alterSql = "alter table %s rename column %s to %s";
			boolean onlyRename = false;
			for (ColumnMod mod : change.columnMods) {
				if (mod instanceof RenameColumnMod) {
					RenameColumnMod tmpMod = (RenameColumnMod) mod;
					this.executeDDL(String.format(alterSql, producer.delimit(r.getDatabase(), r.getTable()), producer.delimit(tmpMod.oldName), producer.delimit(tmpMod.newName)));
					onlyRename = true;
				} else if (mod instanceof ChangeColumnMod) {
					ChangeColumnMod tmpMod = (ChangeColumnMod) mod;
					if (!tmpMod.name.equals(tmpMod.definition.getName())) {
						this.executeDDL(String.format(alterSql, producer.delimit(r.getDatabase(), r.getTable()), producer.delimit(tmpMod.name), producer.delimit(tmpMod.definition.getName())));
					}
				}
			}
			if (onlyRename) {
				return true;
			}
		}
		return false;
	}
}
