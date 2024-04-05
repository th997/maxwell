package com.zendesk.maxwell.producer.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.schema.ddl.DDLMap;
import com.zendesk.maxwell.schema.ddl.ResolvedSchemaChange;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.FixedBackOff;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class JdbcAsyncConsumer implements StoppableTask {
	private final Logger LOG = LoggerFactory.getLogger(getClass());
	private final MaxwellContext context;
	private final JdbcProducer jdbcProducer;
	private ConcurrentMessageListenerContainer<String, String> container;
	private ObjectMapper om = new ObjectMapper();

	public JdbcAsyncConsumer(MaxwellContext context) throws IOException {
		this.context = context;
		jdbcProducer = new JdbcProducer(context);
		this.initialize();
	}

	private void initialize() {
		// get config
		Properties kafkaProperties = context.getConfig().kafkaProperties;
		Properties jdbcProperties = context.getConfig().jdbcProperties;
		String topic = context.getConfig().kafkaTopic;
		int numPartitions = Integer.parseInt(jdbcProperties.getProperty("consumer.numPartitions", "10"));
		short replicationFactor = Short.parseShort(jdbcProperties.getProperty("consumer.replicationFactor", "2"));
		String group = jdbcProperties.getProperty("consumer.group", topic + "-group");
		// kafka consumer
		kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		Map<String, Object> props = Utils.propsToMap(kafkaProperties);
		DefaultKafkaConsumerFactory<Object, Object> defaultFactory = new DefaultKafkaConsumerFactory<>(props);
		// create or modify topic
		this.createOrModifyTopics(props, topic, numPartitions, replicationFactor);
		TopicPartitionOffset[] partitionOffsets = new TopicPartitionOffset[numPartitions];
		for (int i = 0; i < numPartitions; i++) {
			partitionOffsets[i] = new TopicPartitionOffset(topic, i);
		}
		ContainerProperties containerProperties = new ContainerProperties(partitionOffsets);
		containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
		containerProperties.setMessageListener(new BinLogMessageListener());
		container = new ConcurrentMessageListenerContainer<>(defaultFactory, containerProperties);
		// one thread per partition
		container.setConcurrency(numPartitions);
		// Retry immediately without delay，and if retry times exceeds 2 times, throw exception to stop
		container.setCommonErrorHandler(new DefaultErrorHandler((record, exception) -> {
			throw new RuntimeException(exception);
		}, new FixedBackOff(0L, 2L)));
		container.start();
	}

	public JdbcProducer getJdbcProducer() {
		return jdbcProducer;
	}

	@Override
	public void requestStop() throws Exception {
		container.pause();
		jdbcProducer.requestStop();
		if (container != null) {
			container.stop();
		}
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		jdbcProducer.awaitStop(timeout);
	}

	class BinLogMessageListener implements AcknowledgingMessageListener<String, String> {
		@Override
		public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
			RowMap rowMap;
			try {
				JsonNode node = om.readTree(record.value());
				String type = node.get("type").asText();
				if ("ddl".equals(type)) {
					ResolvedSchemaChange change = om.readValue(node.get("change").toString(), ResolvedSchemaChange.class);
					rowMap = new DDLMap(change, node.get("ts").asLong() * 1000, node.get("sql").asText(), null, null, node.get("schemaId").asLong());
				} else {
					List<String> keys = null;
					if (node.has("primary_key_columns")) {
						keys = om.readValue(node.get("primary_key_columns").toString(), ArrayList.class);
					}
					rowMap = new RowMap(type, node.get("database").asText(), node.get("table").asText(), node.get("ts").asLong() * 1000, keys, null, null, null);
					if (node.has("data")) {
						Map<String, Object> data = om.readValue(node.get("data").toString(), HashMap.class);
						rowMap.getData().putAll(data);
					}
					if (node.has("old")) {
						Map<String, Object> old = om.readValue(node.get("old").toString(), HashMap.class);
						rowMap.getOldData().putAll(old);
					}
					if (node.has("commit") && node.get("commit").asBoolean()) {
						rowMap.setTXCommit();
					}
				}
				rowMap.setBindObject(acknowledgment);
				getJdbcProducer().push(rowMap);
			} catch (Exception e) {
				LOG.error("parse record error: {}", record, e);
				throw new RuntimeException(e);
			}
		}
	}

	private void createOrModifyTopics(Map<String, Object> props, String topic, int numPartitions, short replicationFactor) {
		try (AdminClient adminClient = AdminClient.create(props)) {
			ListTopicsOptions options = new ListTopicsOptions();
			options.listInternal(false);
			ListTopicsResult topics = adminClient.listTopics(options);
			try {
				Set<String> topicNames = topics.names().get();
				if (!topicNames.contains(topic)) {
					KafkaAdmin admin = new KafkaAdmin(props);
					admin.createOrModifyTopics(new NewTopic(topic, numPartitions, replicationFactor));
				}
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
