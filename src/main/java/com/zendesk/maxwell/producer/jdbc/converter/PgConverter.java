package com.zendesk.maxwell.producer.jdbc.converter;

import com.zendesk.maxwell.producer.jdbc.TableColumn;
import org.apache.commons.lang.StringEscapeUtils;

import java.util.*;

public class PgConverter implements Converter {
	private final static Map<String, String> typeMap = new HashMap<>();

	// https://dev.mysql.com/doc/refman/5.7/en/data-types.html
	// https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
	// see also ColumnDef.java
	static {
		typeMap.put("bigint", "int8");
		typeMap.put("int", "int4");
		typeMap.put("mediumint", "int4");
		typeMap.put("smallint", "int2");
		typeMap.put("tinyint", "int2");
		typeMap.put("double", "float8");
		typeMap.put("float", "float4");
		typeMap.put("decimal", "numeric");
		typeMap.put("bit", "int8");

		typeMap.put("enum", "varchar");
		typeMap.put("set", "varchar");
		typeMap.put("char", "bpchar");
		typeMap.put("varchar", "varchar");
		typeMap.put("text", "text");
		typeMap.put("tinytext", "text");
		typeMap.put("mediumtext", "text");
		typeMap.put("longtext", "text");

		typeMap.put("datetime", "timestamp");
		typeMap.put("timestamp", "timestamp");
		typeMap.put("date", "date");
		typeMap.put("time", "time");
		typeMap.put("year", "int2");

		typeMap.put("blob", "bytea");
		typeMap.put("tinyblob", "bytea");
		typeMap.put("mediumblob", "bytea");
		typeMap.put("longblob", "bytea");
		typeMap.put("binary", "bytea");
		typeMap.put("varbinary", "bytea");
	}

	private final TableColumn source;

	public PgConverter(TableColumn source) {
		this.source = source;
	}

	// compare column if is same
	@Override
	public boolean isEqualsTargetCol(TableColumn c) {
		return isSameType(c) && isSameNullAble(c) && isSameDefault(c);
	}

	@Override
	public boolean isSameType(TableColumn c) {
		return Objects.equals(source.getColumnName(), c.getColumnName()) //
			&& (Objects.equals(source.getDataType(), c.getDataType()) || Objects.equals(this.typeGet(source.getDataType()), c.getDataType())) //
			&& (Objects.equals(source.getStrLen(), c.getStrLen()) || source.getStrLen() == null || c.getStrLen() == null) //
			&& (Objects.equals(source.getNumericPrecision(), c.getNumericPrecision()) && Objects.equals(source.getNumericScale(), c.getNumericScale()) || !"decimal".equals(source.getDataType()));
	}

	@Override
	public boolean isSameNullAble(TableColumn c) {
		String type = this.typeGet(source.getDataType());
		return Objects.equals(source.isNullAble(), c.isNullAble()) || (type.contains("timestamp") && c.isNullAble());
	}

	@Override
	public boolean isSameDefault(TableColumn c) {
		return isSameType(c) && (Objects.equals(source.getColumnDefault(), c.getColumnDefault())
			|| (c.getColumnDefault() != null && c.getColumnDefault().startsWith("nextval(")) // auto_increment
			|| Objects.equals(source.getColumnDefault(), getPostgresDefaultStr(c.getColumnDefault())) // value same
			|| Objects.equals(getDefaultStr(), getPostgresDefaultStr(c.getColumnDefault())) // value same
		);
	}

	private String getPostgresDefaultStr(String columnDefault) {
		if (source.getColumnDefault() == null) {
			return null;
		}
		List<String> chs = Arrays.asList("::character varying", "::bpchar", "::text", "::timestamp without time zone", "::integer");
		for (String ch : chs) {
			if (source.getColumnDefault().endsWith(ch)) {
				columnDefault = columnDefault.substring(0, columnDefault.length() - ch.length());
				if (source.getColumnDefault().length() > 1 && columnDefault.startsWith("'") && columnDefault.endsWith("'")) {
					columnDefault = columnDefault.substring(1, columnDefault.length() - 1);
				} else if (ch.equals("NULL")) {
					columnDefault = null;
				}
				break;
			}
		}
		return columnDefault;
	}


	@Override
	public String toTargetCol() {
		StringBuilder tempSql = new StringBuilder("\"" + source.getColumnName() + "\" ");
		tempSql.append(this.toColType());
		tempSql.append(this.toColNullAble());
		tempSql.append(this.toColDefault());
		return tempSql.toString().trim();
	}

	@Override
	public String typeGet() {
		return typeGet(source.getDataType());
	}

	private String typeGet(String dataType) {
		String type = typeMap.getOrDefault(source.getDataType(), dataType);
		if (source.getColumnType().endsWith("unsigned") && !source.isPri()) {
			if (source.getDataType().equals("smallint")) {
				type = "int4";
			} else if (source.getDataType().equals("int")) {
				type = "int8";
			} else if (source.getDataType().equals("bigint")) {
				type = "numeric";
				source.setNumericPrecision(21);
				source.setNumericScale(0);
			} else if ((source.getDataType().equals("double") || dataType.equals("float")) && source.getNumericPrecision() != null && source.getNumericScale() != null) {
				type = "numeric";
			}
		}
		return type;
	}

	@Override
	public String toColType() {
		StringBuilder tempSql = new StringBuilder();
		String type = this.typeGet(source.getDataType());
		if (source.isAutoIncrement()) {
			if ("bigint".equals(source.getDataType())) {
				type = "bigserial";
			} else {
				type = "serial";
			}
		}
		if (source.getStrLen() != null && type.contains("char")) {
			tempSql.append(String.format("%s(%s) ", type, source.getStrLen()));
		} else if (type.equals("numeric") && source.getNumericPrecision() != null && source.getNumericScale() != null) {
			tempSql.append(String.format("numeric(%s,%s) ", source.getNumericPrecision(), source.getNumericScale()));
		} else {
			tempSql.append(type + " ");
		}
		return tempSql.toString();
	}

	public String toColNullAble() {
		String type = this.typeGet(source.getDataType());
		if (source.isNullAble() || type.contains("timestamp")) {
			return "null ";
		} else {
			return "not null ";
		}
	}

	@Override
	public String toColDefault() {
		String v = this.getDefaultStr();
		return v == null ? "" : String.format("default %s ", v);
	}

	public String getDefaultStr() {
		String type = this.typeGet(source.getDataType());
		if (source.getColumnDefault() != null) {
			if (type.contains("char") || type.contains("text")) {
				return String.format("'%s'", StringEscapeUtils.escapeSql(source.getColumnDefault()));
			} else if (type.contains("timestamp") && source.getColumnDefault().matches("\\d{4}-[\\s\\S]*")) { // time like ‘2099-01-01 00:00:00’
				if (!source.getColumnDefault().startsWith("0000")) {
					return String.format("'%s'", source.getColumnDefault());
				} else {
					return null;
				}
			} else if (source.getDataType().contains("bit")) {
				return Long.valueOf(source.getColumnDefault().replaceAll("b|'", ""), 2).toString();
			} else {
				return source.getColumnDefault();
			}
		}
		return null;
	}
}
