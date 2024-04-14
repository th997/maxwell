package com.zendesk.maxwell.producer.jdbc;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import org.junit.Before;
import org.junit.Test;

public class DataCompareLogicTest {

	private JdbcProducer jdbcProducer;

	@Before
	public void init() throws Exception {
		MaxwellConfig config = new MaxwellConfig(new String[]{});
		MaxwellContext context = new MaxwellContext(config);
		jdbcProducer = new JdbcProducer(context);
	}

	@Test
	public void testCompare() {
		jdbcProducer.dataCompareLogic.compare(null);
	}
}
