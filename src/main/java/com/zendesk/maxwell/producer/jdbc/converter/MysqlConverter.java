package com.zendesk.maxwell.producer.jdbc.converter;

import com.zendesk.maxwell.producer.jdbc.TableColumn;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

public class MysqlConverter implements Converter {
	private final TableColumn source;

	public MysqlConverter(TableColumn source) {
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
			&& (Objects.equals(source.getDataType(), c.getDataType()) || Objects.equals(this.typeGet(source.getDataType()), c.getDataType()))//
			&& (Objects.equals(source.getStrLen(), c.getStrLen()) || source.getStrLen() == null || c.getStrLen() == null)//
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
		);
	}

	@Override
	public String toTargetCol() {
		StringBuilder tempSql = new StringBuilder("`" + source.getColumnName() + "` ");
		tempSql.append(this.toColType());
		tempSql.append(this.toColNullAble());
		tempSql.append(this.toColDefault());
		if (source.isAutoIncrement()) {
			tempSql.append("auto_increment");
		}
		if (StringUtils.isNotEmpty(source.getColumnComment())) {
			tempSql.append(String.format(" comment '%s'", StringEscapeUtils.escapeSql(source.getColumnComment())));
		}
		return tempSql.toString().trim();
	}

	@Override
	public String typeGet() {
		return source.getDataType();
	}

	private String typeGet(String dataType) {
		return source.getDataType();
	}

	@Override
	public String toColType() {
		return source.getColumnType() + " ";
	}

	public String toColNullAble() {
		if (source.isNullAble() || source.getDataType().equals("timestamp") && source.getColumnDefault() == null) {
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

	private String getDefaultStr() {
		String type = source.getDataType();
		if (source.getColumnDefault() != null) {
			if (type.contains("char") || type.contains("text") || type.contains("set") || type.contains("enum")) {
				return String.format("'%s'", StringEscapeUtils.escapeSql(source.getColumnDefault()));
			} else if (type.contains("timestamp") || type.contains("datetime") && source.getColumnDefault().matches("\\d{4}-[\\s\\S]*")) { // time like ‘2099-01-01 00:00:00’
				if ("CURRENT_TIMESTAMP".equalsIgnoreCase(source.getColumnDefault())) {
					return source.getColumnDefault();
				} else if (!source.getColumnDefault().startsWith("0000")) {
					return String.format("'%s'", source.getColumnDefault());
				} else {
					return null;
				}
			} else {
				return source.getColumnDefault();
			}
		}
		return null;
	}
}
