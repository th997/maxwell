package com.zendesk.maxwell.producer.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpVersion;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class DorisLogic {
	final JdbcProducer jdbcProducer;
	final CloseableHttpClient httpClient;
	final Properties properties;
	final ObjectMapper om;
	final String httpAddress;
	final Integer connectionTimeout;
	final Integer readTimeout;

	public DorisLogic(JdbcProducer jdbcProducer) {
		this.jdbcProducer = jdbcProducer;
		properties = jdbcProducer.properties;
		om = jdbcProducer.om;
		httpAddress = properties.getProperty("fe.httpAddress");
		connectionTimeout = Integer.parseInt(properties.getProperty("fe.connectionTimeout", "5000"));
		readTimeout = Integer.parseInt(properties.getProperty("fe.readTimeout", "60000"));
		this.httpClient = HttpClients.custom()
			.setDefaultRequestConfig(RequestConfig.custom()
				.setConnectTimeout(connectionTimeout)
				.setSocketTimeout(readTimeout)
				.build())
			.setRedirectStrategy(new DefaultRedirectStrategy() {
				@Override
				protected boolean isRedirectable(String method) {
					return true;
				}
			})
			.build();
	}

	public void streamLoad(String schema, String table, String[] columnNames, List<Object[]> argsList) {
		List<Map<String, Object>> list = new ArrayList<>(argsList.size());
		for (Object[] args : argsList) {
			Map<String, Object> map = new HashMap<>(columnNames.length);
			for (int i = 0; i < columnNames.length; i++) {
				map.put(columnNames[i], args[i]);
			}
			list.add(map);
		}
		this.streamLoad(schema, table, list);
	}

	public void streamLoad(String schema, String table, List<Map<String, Object>> list) {
		try {
			String body = om.writeValueAsString(list);
			String url = String.format("%s/api/%s/%s/_stream_load", httpAddress, schema, table);
			String user = properties.getProperty("user");
			String password = properties.getProperty("password", "");
			String label = schema + "_" + table + System.currentTimeMillis();
			HttpPut httpPut = new HttpPut(url);
			httpPut.setHeader("label", label);
			httpPut.setHeader("format", "json");
			httpPut.setHeader("strip_outer_array", "true");
			httpPut.setHeader("strict_mode", "true");
			httpPut.setHeader("Expect", "100-continue");
			httpPut.setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(String.format("%s:%s", user, password).getBytes()));
			httpPut.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
			httpPut.setProtocolVersion(HttpVersion.HTTP_1_0);
			try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
				int statusCode = response.getStatusLine().getStatusCode();
				String res = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
				if (statusCode != 200) {
					throw new RuntimeException("streamLoad failed,database=" + schema + ",table=" + table + ",statusCode=" + statusCode + ",res=" + res);
				} else {
					JsonNode resNode = om.readTree(res);
					if (!resNode.has("Status") || !"Success".equals(resNode.get("Status").asText())) {
						throw new RuntimeException("streamLoad failed,database=" + schema + ",table=" + table + ",statusCode=" + statusCode + ",res=" + res);
					}
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void stop() throws IOException {
		this.httpClient.close();
	}
}
