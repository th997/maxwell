package com.zendesk.maxwell.producer.jdbc;

import com.google.common.base.Equivalence;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.SingleColumnRowMapper;

import java.util.*;
import java.util.stream.Collectors;

public class DataCompareLogic {
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	final JdbcProducer producer;
	private BeanPropertyRowMapper<TableColumn> columnRowMapper = BeanPropertyRowMapper.newInstance(TableColumn.class);

	public DataCompareLogic(JdbcProducer producer) {
		this.producer = producer;
	}

	public Set<String> compare(Set<String> diffTables) {
		Set<String> ret = new HashSet<>();
		for (String db : producer.syncDbs) {
			Set<String> diffSet = this.compare(db, diffTables, 0);
			ret.addAll(diffSet);
		}
		return ret;
	}

	public Set<String> compare(String db, Set<String> diffTables, Integer count) {
		LOG.info("data compare start, db={}", db);
		long start = System.currentTimeMillis();
		String sqlTables = "select table_name,column_name,column_type,data_type,column_comment,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' as \"auto_increment\" from information_schema.columns t where t.table_schema =?";
		String sqlCount = "select %s from `%s`.`%s` t";
		if (diffTables == null) {
			diffTables = new HashSet<>();
		}
		if (!diffTables.isEmpty()) {
			sqlTables = sqlTables + " and table_name in ('" + String.join("','", diffTables) + "')";
		}
		diffTables.clear();
		List<TableColumn> columns = producer.mysqlJdbcTemplate.query(sqlTables, columnRowMapper, db);
		Map<String, List<TableColumn>> tableMap = columns.stream().collect(Collectors.groupingBy(TableColumn::getTableName));
		Map<String, Integer> tableCountMap = this.queryTableCount(db);
		for (Map.Entry<String, List<TableColumn>> e : tableMap.entrySet()) {
			String table = e.getKey();
			if (tableCountMap.getOrDefault(table, 0) > producer.dataCompareRowsLimit * 10) {
				LOG.info("compare ignore,table={},count={}", table, tableCountMap.get(table));
				continue;
			}
			StringBuilder str = new StringBuilder("count(*) as _count");
			boolean hasPriKey = false;
			for (TableColumn column : e.getValue()) {
				if (column.isPri()) {
					hasPriKey = true;
				}
				if (tableCountMap.getOrDefault(table, 0) < producer.dataCompareRowsLimit) {
					if (column.getDataType().contains("int") || column.getDataType().contains("decimal") || column.getDataType().contains("float") || column.getDataType().contains("double")) {
						if (producer.isDoris() && column.getDataType().equals("bigint")) {
							str.append(String.format(",sum(cast(t.`%s` as decimal(30))) as %s", column.getColumnName(), column.getColumnName().toLowerCase()));
						} else {
							str.append(String.format(",sum(t.`%s`) as %s", column.getColumnName(), column.getColumnName().toLowerCase()));
						}
					}
				}
			}
			if (!hasPriKey) {
				continue;
			}
			String sql = String.format(sqlCount, str, db, table);
			Map<String, Object> map1 = producer.mysqlJdbcTemplate.queryForMap(sql);
			if ('`' != producer.quote()) {
				sql = sql.replace('`', producer.quote());
			}
			Map<String, Object> map2 = producer.targetJdbcTemplate.queryForMap(sql);
			MapDifference diff = Maps.difference(map1, map2, new MyEquivalence());
			if (!diff.areEqual()) {
				diffTables.add(table);
				Map<String, MapDifference.ValueDifference<Object>> diffMap = diff.entriesDiffering();
				for (String key : diffMap.keySet()) {
					MapDifference.ValueDifference<Object> vd = diffMap.get(key);
					LOG.info(String.format("diff table=%s,key=%s,v1=%s,v2=%s", table, key, vd.leftValue(), vd.rightValue()));
				}
				LOG.info(sql);
			} else {
				// LOG.info("data compare table is same,db={},table={}", db, table);
			}
		}
		LOG.info("data compare end,db={},tableCount={},time={},same={}", db, tableMap.size(), System.currentTimeMillis() - start, diffTables.isEmpty());
		if (!diffTables.isEmpty() && count < 2) {
			try {
				Thread.sleep(producer.dataCompareSecond * 1000);
			} catch (InterruptedException e) {
			}
			return compare(db, diffTables, ++count);
		}
		LOG.info("diff tables=\"" + String.join("\",\"", diffTables) + "\"");
		for (String table : diffTables) {
			this.dataDiff(db, table, tableMap.get(table), 0L);
		}
		return diffTables;
	}

	public void dataDiff(String db, String table, List<TableColumn> columns, Long start) {
		TableColumn pri = columns.stream().filter(TableColumn::isPri).findFirst().orElse(null);
		if (pri == null || !pri.getDataType().contains("int")) {
			return;
		}
		Integer limit = producer.dataCompareRowsLimit;
		String col = pri.getColumnName();
		String sql = String.format("select `%s` from `%s`.`%s` where `%s`>%s order by `%s` asc limit %s", col, db, table, col, start, col, limit);
		List<Long> list1 = producer.mysqlJdbcTemplate.query(sql, SingleColumnRowMapper.newInstance(Long.class));
		Set<Long> set1 = new HashSet<>(list1);
		List<Long> list2 = producer.targetJdbcTemplate.query(sql.replace('`', producer.quote()), SingleColumnRowMapper.newInstance(Long.class));
		Set<Long> set2 = new HashSet<>(list2);
		Set<Long> diff1 = set1.stream().filter(e -> !set2.contains(e)).collect(Collectors.toSet());
		Set<Long> diff2 = set2.stream().filter(e -> !set1.contains(e)).collect(Collectors.toSet());
		if (diff1.isEmpty() && diff2.isEmpty() && list1.size() == limit) {
			dataDiff(db, table, columns, list1.get(list1.size() - 1));
			return;
		}
		if (!diff1.isEmpty()) { // add
			String insertSql = "insert into `%s`.`bootstrap` (database_name, table_name, where_clause, client_id) values('%s','%s','%s','%s')";
			String maxwellDb = producer.context.getConfig().maxwellMysql.database;
			String maxwellClient = producer.context.getConfig().clientID;
			String where = String.format("`%s` in (%s)", pri.getColumnName(), StringUtils.join(diff1, ","));
			insertSql = String.format(insertSql, maxwellDb, db, table, where, maxwellClient);
			LOG.info("dataDiff only mysql,db={},table={},updateSql=\n{}", db, table, insertSql);

		}
		if (!diff2.isEmpty()) { // delete
			String deleteSql = "delete from %s where %s in (%s)";
			deleteSql = String.format(deleteSql, producer.delimit(db, table), producer.delimit(pri.getColumnName()), StringUtils.join(diff2, ","));
			LOG.info("dataDiff only target,db={},table={},updateSql=\n{}", db, table, deleteSql);
		}
		if (diff1.isEmpty() && diff2.isEmpty()) {
			String deleteSql = String.format("truncate table %s", producer.delimit(db, table));
			String maxwellDb = producer.context.getConfig().maxwellMysql.database;
			String maxwellClient = producer.context.getConfig().clientID;
			String insertSql = String.format("insert into `%s`.`bootstrap` (database_name, table_name, client_id) values('%s','%s','%s')", maxwellDb, db, table, maxwellClient);
			Integer count = producer.targetJdbcTemplate.queryForObject(String.format("select count(*) from %s", producer.delimit(db, table)), Integer.class);
			LOG.info("dataDiff only diff,db={},table={},count={},updateSql=\n{}\n{}", db, table, count, deleteSql, insertSql);
		}
	}

	private Map<String, Integer> queryTableCount(String db) {
		Map<String, Integer> ret = new HashMap<>();
		String sqlTablePre = "select table_name, table_rows from information_schema.tables where table_schema=?";
		List<Map<String, Object>> list = producer.mysqlJdbcTemplate.queryForList(sqlTablePre, db);
		for (Map<String, Object> map : list) {
			Object tableName = map.get("table_name");
			Object tableRows = map.get("table_rows");
			if (tableRows != null) {
				ret.put(tableName.toString(), Integer.parseInt(tableRows.toString()));
			}
		}
		return ret;
	}


	static class MyEquivalence extends Equivalence {
		@Override
		protected boolean doEquivalent(Object o1, Object o2) {
			if (Objects.equals(o1, o2)) {
				return true;
			}
			if (o1 == null && o2 != null) {
				return false;
			}
			if (o1 != null && o2 == null) {
				return false;
			}
			if (o1 instanceof Number && o2 instanceof Number) {
				Number n1 = (Number) o1;
				Number n2 = (Number) o2;
				return Objects.equals(n1, n2) || n1.doubleValue() == ((Number) o2).doubleValue() || o1.toString().equals(o2.toString()) //
					|| Math.round(n1.doubleValue() * 100) == Math.round(n2.doubleValue() * 100);
			}
			if (o1 instanceof Boolean || o2 instanceof Boolean) {
				if (o1 instanceof Number) {
					return o2.equals(((Number) o1).intValue() > 0);
				}
				if (o2 instanceof Number) {
					return o1.equals(((Number) o2).intValue() > 0);
				}
			}
			return false;
		}

		@Override
		protected int doHash(Object o) {
			return 0;
		}
	}

}
