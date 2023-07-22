package com.zendesk.maxwell.monitoring;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.collect.ImmutableMap;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class BinlogDelayGaugeSet implements MetricSet {
	static final Logger LOG = LoggerFactory.getLogger(BinlogDelayGaugeSet.class);

	private MaxwellContext context;

	public BinlogDelayGaugeSet(MaxwellContext context) {
		this.context = context;

	}

	@Override
	public Map<String, Metric> getMetrics() {
		return ImmutableMap.of( //
			"delay.second", (Gauge) () -> this.getDelaySecond(),//
			"delay.delay", (Gauge) () -> this.getDelaySecond() > context.getConfig().metricsConsumerDelayAlert
		);
	}

	private int getDelaySecond() {
		Position position = null;
		try {
			position = context.getPosition();
		} catch (Exception e) {
			LOG.error("BinlogDelayGaugeSet error", e);
		}
		int delay = 0;
		if (position != null) {
			delay = (int) ((System.currentTimeMillis() - position.getLastHeartbeatRead()) / 1000);
		}
		return delay;
	}
}
