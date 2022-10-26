package com.zendesk.maxwell.producer.postgresql;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.*;

public class TableColumn {
	private static Map<String, String> typeMap = new HashMap<>();

	static {
		typeMap.put("bigint", "int8");
		typeMap.put("int", "int4");
		typeMap.put("mediumint", "int4");
		typeMap.put("smallint", "int2");
		typeMap.put("tinyint", "int2");
		typeMap.put("double", "float8");
		typeMap.put("float", "float4");
		typeMap.put("decimal", "numeric");

		typeMap.put("char", "bpchar");
		typeMap.put("enum", "varchar");
		typeMap.put("set", "varchar");
		typeMap.put("tinytext", "text");
		typeMap.put("mediumtext", "text");
		typeMap.put("longtext", "text");

		typeMap.put("datetime", "timestamp");

		typeMap.put("blob", "bytea");
		typeMap.put("tinyblob", "bytea");
		typeMap.put("mediumblob", "bytea");
		typeMap.put("longblob", "bytea");
		typeMap.put("binary", "bytea");

	}

	private String columnName;
	private String dataType;
	private Long strLen;
	private Integer numericPrecision;
	private Integer numericScale;
	private String columnDefault;
	private boolean nullAble;
	private boolean pri;
	private boolean autoIncrement;
	private String columnComment;

	// compare column if is same
	public boolean equalsPostgresCol(TableColumn c) {
		return isSameType(c) && isSameNullAble(c) && isSameDefault(c);
	}

	public boolean isSameType(TableColumn c) {
		return Objects.equals(columnName, c.columnName)
				&& (Objects.equals(dataType, c.dataType) || Objects.equals(typeMap.get(dataType), c.dataType))
				&& (Objects.equals(strLen, c.strLen) || strLen == null || c.strLen == null);
	}

	public boolean isSameNullAble(TableColumn c) {
		return Objects.equals(nullAble, c.nullAble);
	}

	public boolean isSameDefault(TableColumn c) {
		String type = typeMap.getOrDefault(dataType, dataType);
		return Objects.equals(columnDefault, c.columnDefault)
				|| (c.columnDefault != null && c.columnDefault.startsWith("nextval(")) // auto_increment
				|| Objects.equals(columnDefault, getPostgresDefaultStr(c.columnDefault)) // value same
				|| (type.equals("timestamp") && "0000-00-00 00:00:00".equals(columnDefault) && c.columnDefault == null)
				;
	}

	private String getPostgresDefaultStr(String columnDefault) {
		if (columnDefault == null) {
			return null;
		}
		List<String> chs = Arrays.asList("::character varying", "::bpchar");
		for (String ch : chs) {
			if (columnDefault.endsWith(ch)) {
				columnDefault = columnDefault.substring(0, columnDefault.length() - ch.length());
				if (columnDefault.length() > 1 && columnDefault.startsWith("'") && columnDefault.endsWith("'")) {
					columnDefault = columnDefault.substring(1, columnDefault.length() - 1);
				} else if (ch.equals("NULL")) {
					columnDefault = null;
				}
				break;
			}
		}
		return columnDefault;
	}

	// to postgres column define
	public String toPostgresCol() {
		StringBuilder tempSql = new StringBuilder("\"" + columnName + "\" ");
		tempSql.append(this.toColType());
		tempSql.append(this.toColNullAble());
		tempSql.append(this.toColDefault());
		return tempSql.toString().trim();
	}

	public String toColType() {
		StringBuilder tempSql = new StringBuilder();
		String type = typeMap.getOrDefault(dataType, dataType);
		if (autoIncrement) {
			if ("bigint".equals(dataType)) {
				type = "bigserial";
			} else {
				type = "serial";
			}
		}
		if (strLen != null && type.contains("char")) {
			tempSql.append(String.format("%s(%s) ", type, strLen));
		} else if (dataType.equals("bit") && numericPrecision != null && numericPrecision == 1) {
			tempSql.append("int2 ");
		} else if (dataType.equals("decimal") && numericPrecision != null && numericScale != null) {
			tempSql.append(String.format("decimal(%s,%s) ", numericPrecision, numericScale));
		} else if (dataType.equals("bit")) {
			tempSql.append("int8 ");
		} else {
			tempSql.append(type + " ");
		}
		return tempSql.toString();
	}

	public String toColNullAble() {
		if (nullAble) {
			return "null ";
		} else {
			return "not null ";
		}
	}

	public String toColDefault() {
		StringBuilder tempSql = new StringBuilder();
		String type = typeMap.getOrDefault(dataType, dataType);
		if (columnDefault != null) {
			if (type.contains("char")) {
				tempSql.append(String.format("default '%s' ", StringEscapeUtils.escapeSql(columnDefault)));
			} else if (type.contains("timestamp") && columnDefault.matches("\\d{4}-[\\s\\S]*")) { // time like ‘2099-01-01 00:00:00’
				if (columnDefault.startsWith("0000")) {
					tempSql.append(String.format("default null "));
				} else {
					tempSql.append(String.format("default '%s' ", columnDefault));
				}
			} else if (dataType.contains("bit")) {
				tempSql.append(String.format("default %s ", Long.valueOf(columnDefault.replaceAll("b|'", ""), 2)));
			} else {
				tempSql.append(String.format("default %s ", columnDefault));
			}
		}
		return tempSql.toString();
	}

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
}
