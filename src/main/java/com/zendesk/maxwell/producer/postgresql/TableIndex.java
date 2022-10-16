package com.zendesk.maxwell.producer.postgresql;

public class TableIndex {
	private String keyName;
	private String columnName;
	private String indexType;
	private boolean nonUnique;
	private String indexDef;

	public String getKeyName() {
		return keyName;
	}

	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public boolean isNonUnique() {
		return nonUnique;
	}

	public void setNonUnique(boolean nonUnique) {
		this.nonUnique = nonUnique;
	}

	public String getIndexDef() {
		return indexDef;
	}

	public void setIndexDef(String indexDef) {
		this.indexDef = indexDef;
	}
}
