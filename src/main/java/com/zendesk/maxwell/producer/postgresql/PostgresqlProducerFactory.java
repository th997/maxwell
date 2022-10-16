package com.zendesk.maxwell.producer.postgresql;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.ProducerFactory;

public class PostgresqlProducerFactory implements ProducerFactory {
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		return new PostgresqlProducer(context);
	}
}
