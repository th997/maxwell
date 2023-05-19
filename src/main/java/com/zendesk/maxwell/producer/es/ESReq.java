package com.zendesk.maxwell.producer.es;

import com.zendesk.maxwell.row.RowMap;
import org.elasticsearch.action.DocWriteRequest;

public class ESReq {
	private RowMap rowMap;
	private DocWriteRequest<?> req;

	public ESReq(RowMap rowMap, DocWriteRequest<?> req) {
		this.rowMap = rowMap;
		this.req = req;
	}

	public RowMap getRowMap() {
		return rowMap;
	}

	public void setRowMap(RowMap rowMap) {
		this.rowMap = rowMap;
	}

	public DocWriteRequest<?> getReq() {
		return req;
	}

	public void setReq(DocWriteRequest<?> req) {
		this.req = req;
	}
}
