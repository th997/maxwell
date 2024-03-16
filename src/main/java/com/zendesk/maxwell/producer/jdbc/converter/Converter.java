package com.zendesk.maxwell.producer.jdbc.converter;

import com.zendesk.maxwell.producer.jdbc.TableColumn;

public interface Converter {
	static Converter getConverter(TableColumn source, String type) {
		if ("postgresql".equals(type)) {
			return new PgConverter(source);
		} else if ("doris".equals(type)) {
			return new DorisConverter(source);
		} else if ("starrocks".equals(type)) {
			return new DorisConverter(source);
		} else if ("mysql".equals(type)) {
			return new MysqlConverter(source);
		}
		return null;
	}

	// compare column if is same
	boolean isEqualsTargetCol(TableColumn c);

	boolean isSameType(TableColumn target);

	boolean isSameNullAble(TableColumn target);

	boolean isSameDefault(TableColumn target);

	String toTargetCol();
	String toTargetCol(TableColumn target);

	String typeGet();

	String toColType();

	String toColDefault();
}
