package com.zendesk.maxwell.producer.jdbc;

import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class UpdateSqlGroup {
	private String sql;
	private List<Object[]> argsList = new ArrayList<>();
	private RowMap lastRowMap;
	private List<String> sqlWithArgsList = new ArrayList<>();
	private List<Map<String, Object>> dataList = new ArrayList<>();

	public UpdateSqlGroup(String sql) {
		this.sql = sql;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public List<Object[]> getArgsList() {
		return argsList;
	}

	public void setArgsList(List<Object[]> argsList) {
		this.argsList = argsList;
	}

	public RowMap getLastRowMap() {
		return lastRowMap;
	}

	public void setLastRowMap(RowMap lastRowMap) {
		this.lastRowMap = lastRowMap;
	}

	public List<String> getSqlWithArgsList() {
		return sqlWithArgsList;
	}

	public void setSqlWithArgsList(List<String> sqlWithArgsList) {
		this.sqlWithArgsList = sqlWithArgsList;
	}

	public List<Map<String, Object>> getDataList() {
		return dataList;
	}

	public void setDataList(List<Map<String, Object>> dataList) {
		this.dataList = dataList;
	}
}
