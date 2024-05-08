package com.zendesk.maxwell.producer.jdbc;

public class TableColumn {
	private String tableName;
	private String columnName;
	private String columnType;
	private String dataType;
	private Long strLen;
	private Integer numericPrecision;
	private Integer numericScale;
	private String columnDefault;
	private boolean nullAble;
	private boolean pri;
	private boolean autoIncrement;
	private String columnComment;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public Long getStrLen() {
		return strLen;
	}

	public void setStrLen(Long strLen) {
		this.strLen = strLen;
	}

	public String getColumnDefault() {
		return columnDefault;
	}

	public void setColumnDefault(String columnDefault) {
		this.columnDefault = columnDefault;
	}

	public boolean isNullAble() {
		return nullAble;
	}

	public void setNullAble(boolean nullAble) {
		this.nullAble = nullAble;
	}

	public boolean isAutoIncrement() {
		return autoIncrement;
	}

	public void setAutoIncrement(boolean autoIncrement) {
		this.autoIncrement = autoIncrement;
	}

	public boolean isPri() {
		return pri;
	}

	public void setPri(boolean pri) {
		this.pri = pri;
	}

	public Integer getNumericPrecision() {
		return numericPrecision;
	}

	public void setNumericPrecision(Integer numericPrecision) {
		this.numericPrecision = numericPrecision;
	}

	public String getColumnComment() {
		return columnComment;
	}

	public void setColumnComment(String columnComment) {
		this.columnComment = columnComment;
	}

	public Integer getNumericScale() {
		return numericScale;
	}

	public void setNumericScale(Integer numericScale) {
		this.numericScale = numericScale;
	}

	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String toString() {
		return "TableColumn{" +
			"tableName='" + tableName + '\'' +
			", columnName='" + columnName + '\'' +
			", columnType='" + columnType + '\'' +
			", dataType='" + dataType + '\'' +
			", strLen=" + strLen +
			", numericPrecision=" + numericPrecision +
			", numericScale=" + numericScale +
			", columnDefault='" + columnDefault + '\'' +
			", nullAble=" + nullAble +
			", pri=" + pri +
			", autoIncrement=" + autoIncrement +
			", columnComment='" + columnComment + '\'' +
			'}';
	}
}
