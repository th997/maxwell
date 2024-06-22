package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.filtering.Filter;
import com.zendesk.maxwell.schema.Schema;

public class TableTruncate extends SchemaChange {
	public String database;
	final String table;

	public TableTruncate(String database, String table) {
		this.database = database;
		this.table = table;
	}

	@Override
	public ResolvedTableTruncate resolve(Schema schema) {
		return new ResolvedTableTruncate(database, table);
	}

	@Override
	public boolean isBlacklisted(Filter filter) {
		if (filter == null) {
			return false;
		} else {
			return filter.isTableBlacklisted(this.database, this.table) || !filter.includes(database, table);
		}
	}

}
