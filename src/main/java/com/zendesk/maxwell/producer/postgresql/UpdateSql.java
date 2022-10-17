package com.zendesk.maxwell.producer.postgresql;

import com.zendesk.maxwell.row.RowMap;

public class UpdateSql {
	private String sql;
	private Object[] args;
	private RowMap rowMap;

	public UpdateSql(String sql, Object[] args, RowMap rowMap) {
		this.sql = sql;
		this.args = args;
		this.rowMap = rowMap;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public Object[] getArgs() {
		return args;
	}

	public void setArgs(Object[] args) {
		this.args = args;
	}

	public RowMap getRowMap() {
		return rowMap;
	}

	public void setRowMap(RowMap rowMap) {
		this.rowMap = rowMap;
	}
}
