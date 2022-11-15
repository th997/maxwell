package com.zendesk.maxwell.producer.postgresql;

import com.zendesk.maxwell.row.RowMap;

import java.util.ArrayList;
import java.util.List;

public class UpdateSqlGroup {
	private String sql;
	private List<Object[]> argsList = new ArrayList<>();
	private RowMap lastRowMap;

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
}
