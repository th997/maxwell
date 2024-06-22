package com.zendesk.maxwell.producer.jdbc.converter;

import com.zendesk.maxwell.producer.jdbc.TableColumn;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class DorisConverter implements Converter {
	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	private final TableColumn source;

	private final boolean isStarRocks;

	public DorisConverter(TableColumn source, String type) {
		this.source = source;
		this.isStarRocks = "starrocks".equals(type);
	}

	// compare column if is same
	@Override
	public boolean isEqualsTargetCol(TableColumn c) {
		return isSameType(c) && isSameNullAble(c) && isSameDefault(c);
	}

	@Override
	public boolean isSameType(TableColumn c) {
		boolean sameName = Objects.equals(source.getColumnName(), c.getColumnName());
		String typeGet = this.typeGet(source.getDataType());
		boolean sameType = Objects.equals(source.getDataType(), c.getDataType()) || Objects.equals(typeGet, c.getDataType()) || (typeGet.startsWith(c.getDataType()) && c.getDataType().equals("varchar")) || Objects.equals(typeGet, c.getColumnType());
		boolean sameLen = Objects.equals(source.getStrLen(), c.getStrLen()) || source.getStrLen() == null || c.getStrLen() == null || c.getColumnType().equalsIgnoreCase("string") || source.getStrLen() * 3 <= c.getStrLen() || c.getStrLen() == 1048576;
		boolean ret = sameName && sameType && sameLen;
		if (!ret) {
			LOG.info("isSameType=false,source={},target={},sameName={},sameType={},sameLen={}", source, c, sameName, sameType, sameLen);
		}
		return ret;
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
		return toTargetCol(null);
	}

	public String toTargetCol(TableColumn target) {
		StringBuilder tempSql = new StringBuilder("`" + source.getColumnName() + "` ");
		tempSql.append(this.toColType());
		tempSql.append(this.toColNullAble(target));
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
		if (source.getDataType().equals("decimal")) {
			type = "decimal(38,18)";
		} else if (source.isAutoIncrement() && source.isPri()) {
			type = "bigint";
		} else if (source.getColumnType().endsWith("unsigned") || source.getColumnType().contains(" unsigned ")) {
			if (source.getDataType().equals("tinyint")) {
				type = "smallint";
			} else if (source.getDataType().equals("mediumint")) {
				type = "int";
			} else if (source.getDataType().equals("int")) {
				type = "bigint";
			} else {
				type = source.getDataType();
			}
		} else if (source.getDataType().equals("enum") //
			|| source.getDataType().equals("set") //
			|| source.getDataType().equals("time") //
			|| source.getDataType().endsWith("text") //
			|| source.getDataType().endsWith("char")) {
			if (source.getStrLen() != null) {
				if (source.isPri()) {
					type = "varchar(" + source.getStrLen() + ")";
				} else if (isStarRocks && source.getStrLen() * 3 <= 1048576) {
					type = "varchar(" + source.getStrLen() * 3 + ")";
				} else if (isStarRocks) {
					type = "varchar(1048576)";
				} else {
					type = "string";
				}
			} else if (source.getDataType().equals("time")) {
				type = "varchar(32)";
			} else {
				type = "string";
			}
		} else if (source.getDataType().endsWith("blob") //
			|| source.getDataType().endsWith("binary")) {
			//type = "varbinary";
			type = isStarRocks ? "varchar(1048576)" : "string";
		} else if (source.getDataType().equals("double") || source.getDataType().equals("float")) {
			type = source.getDataType();
		} else if (source.getDataType().equals("timestamp")) {
			type = "datetime";
		} else if (source.getDataType().equals("bit")) {
			type = "bigint";
		} else if (source.getDataType().equals("year")) {
			type = "smallint";
		} else if (source.getDataType().equals("mediumint")) {
			type = "int";
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

	public String toColNullAble(TableColumn target) {
		if (source.isNullAble() || "timestamp".equals(source.getDataType()) || "datetime".equals(source.getDataType()) || (target != null && target.isNullAble())) {
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
