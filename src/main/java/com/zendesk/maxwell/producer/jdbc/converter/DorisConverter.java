package com.zendesk.maxwell.producer.jdbc.converter;

import com.zendesk.maxwell.producer.jdbc.TableColumn;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class DorisConverter implements Converter {
	private final TableColumn source;

	public DorisConverter(TableColumn source) {
		this.source = source;
	}

	// compare column if is same
	@Override
	public boolean isEqualsTargetCol(TableColumn c) {
		return isSameType(c) && isSameNullAble(c) && isSameDefault(c);
	}

	@Override
	public boolean isSameType(TableColumn c) {
		return Objects.equals(source.getColumnName(), c.getColumnName())//
			&& (Objects.equals(source.getDataType(), c.getDataType()) || Objects.equals(this.typeGet(source.getDataType()), c.getDataType()) || Objects.equals(this.typeGet(source.getDataType()), c.getColumnType()))//
			&& (Objects.equals(source.getStrLen(), c.getStrLen()) || source.getStrLen() == null || c.getStrLen() == null || c.getColumnType().equalsIgnoreCase("string"))//
			&& (Objects.equals(source.getNumericPrecision(), c.getNumericPrecision()) && Objects.equals(source.getNumericScale(), c.getNumericScale()) || !"decimal".equals(source.getDataType()));
	}

	@Override
	public boolean isSameNullAble(TableColumn c) {
		String type = this.typeGet(source.getDataType());
		return Objects.equals(source.isNullAble(), c.isNullAble()) || (type.contains("timestamp") || type.contains("datetime") && c.isNullAble());
	}

	@Override
	public boolean isSameDefault(TableColumn c) {
		return true;
	}

	@Override
	public String toTargetCol() {
		StringBuilder tempSql = new StringBuilder("`" + source.getColumnName() + "` ");
		tempSql.append(this.toColType());
		tempSql.append(this.toColNullAble());
		tempSql.append(this.toColDefault());
		if (StringUtils.isNotEmpty(source.getColumnComment())) {
			tempSql.append(String.format(" comment '%s'", StringEscapeUtils.escapeSql(source.getColumnComment())));
		}
		return tempSql.toString().trim();
	}

	@Override
	public String typeGet() {
		return typeGet(source.getDataType());
	}

	// https://doris.apache.org/zh-CN/docs/lakehouse/multi-catalog/jdbc/?_highlight=unsigned#mysql
	private String typeGet(String dataType) {
		String type = source.getColumnType();
		if (source.getColumnType().endsWith("unsigned") || source.getColumnType().contains(" unsigned ")) {
			if (source.getDataType().equals("tinyint")) {
				type = "smallint";
			} else if (source.getDataType().equals("mediumint")) {
				type = "int";
			} else if (source.getDataType().equals("int")) {
				type = "bigint";
			} else if (source.getDataType().equals("bigint")) {
				if (!source.isPri()) {
					type = "largeint";
				} else {
					type = "bigint";
				}
			} else if (source.getDataType().equals("decimal")) {
				type = source.getColumnType().replace("unsigned", "");
			} else {
				type = source.getDataType();
			}
		} else if (source.getDataType().equals("enum") //
			|| source.getDataType().equals("set") //
			|| source.getDataType().equals("time") //
			|| source.getDataType().equals("text") //
			|| source.getDataType().endsWith("char")) {
			if (source.getStrLen() != null) {
				if (source.getStrLen() * 3 < 65533) {
					type = "varchar(" + source.getStrLen() * 3 + ")";
				} else {
					type = "varbinary";
				}
			} else {
				type = "string";
			}
		} else if (source.getDataType().endsWith("text") //
			|| source.getDataType().endsWith("blob") //
			|| source.getDataType().endsWith("binary")) {
			type = "varbinary";
		} else if (source.getDataType().equals("double") || source.getDataType().equals("float")) {
			type = source.getDataType();
		} else if (source.getDataType().equals("timestamp")) {
			type = "datetime";
		} else if (source.getDataType().equals("bit")) {
			type = "bigint";
		} else if (source.getDataType().equals("year")) {
			type = "smallint";
		}
		if (type.contains("unsigned")) {
			System.out.println(source);
		}
		return type;
	}

	@Override
	public String toColType() {
		return typeGet() + " ";
	}

	public String toColNullAble() {
		if (source.isNullAble() || "timestamp".equals(source.getDataType()) || "datetime".equals(source.getDataType())) {
			return "null ";
		} else {
			return "not null ";
		}
	}

	@Override
	public String toColDefault() {
		return "";
	}
}
