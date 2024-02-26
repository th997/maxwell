package com.zendesk.maxwell.producer.es;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.CaseFormat;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableMap;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.BinlogDelayGaugeSet;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.producer.jdbc.TableColumn;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.HeartbeatRowMap;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * elasticsearch
 */
public class ESProducer extends AbstractProducer implements StoppableTask {
	private static final String SQL_MYSQL_FIELD = "select column_name,column_type,data_type,column_comment,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' auto_increment from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_MYSQL_DBS = "select schema_name from information_schema.schemata";
	private static final String SQL_MYSQL_TABLE = "select table_name from information_schema.tables where table_type != 'VIEW' and table_schema =?";
	private static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final CharMatcher UPPER_CASE_MATCHER = CharMatcher.inRange('A', 'Z');
	static final Logger LOG = LoggerFactory.getLogger(ESProducer.class);
	private ObjectMapper om;
	private Properties esProperties;
	private JdbcTemplate mysqlJdbcTemplate;
	private DataSourceTransactionManager mysqlTransactionManager;
	private RestHighLevelClient client;
	private Deque<ESReq> reqList = new ArrayDeque<>();
	private volatile Long lastUpdate = System.currentTimeMillis();
	private Integer batchLimit;
	private Integer batchTransactionLimit;
	private Set<String> syncDbs;
	private Set<String> logTables;
	private Pattern[] syncTablePatterns;
	private Map<String, Boolean> tableMatchCache = new HashMap<>();
	private Map<String, ESTableConfig[]> syncTableConfig;
	private Map<String, Object> indexSettings;
	private Integer numberOfShards;
	private Integer numberOfReplicas;
	private Integer maxResultWindow;
	// init data
	private boolean initSchemas;
	private boolean initData;
	private boolean initDataLock;
	private Integer initDataThreadNum;
	private Position initPosition = null;

	public ESProducer(MaxwellContext context) throws IOException {
		super(context);
		om = new ObjectMapper();
		esProperties = context.getConfig().esProperties;
		// mysql
		C3P0ConnectionPool sourcePool = (C3P0ConnectionPool) context.getReplicationConnectionPool();
		mysqlJdbcTemplate = new JdbcTemplate(sourcePool.getCpds(), true);
		mysqlTransactionManager = new DataSourceTransactionManager(sourcePool.getCpds());
		// rest client
		String[] uris = StringUtils.split(esProperties.getProperty("uris", ""), ",");
		HttpHost[] hosts = Arrays.stream(uris).map(HttpHost::create).toArray(HttpHost[]::new);
		RestClientBuilder builder = RestClient.builder(hosts).setRequestConfigCallback(callback -> {
			callback.setConnectTimeout(Integer.parseInt(esProperties.getProperty("connectionTimeout", "5000")));
			callback.setSocketTimeout(Integer.parseInt(esProperties.getProperty("readTimeout", "60000")));
			return callback;
		}).setHttpClientConfigCallback(callback -> {
			callback.setMaxConnTotal(Integer.parseInt(esProperties.getProperty("maxConnTotal", "50")));
			callback.setMaxConnPerRoute(Integer.parseInt(esProperties.getProperty("maxConnPerRoute", "15")));
			String userName = esProperties.getProperty("userName");
			String password = esProperties.getProperty("password");
			if (StringUtils.isNoneEmpty(userName, password)) {
				CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
				credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
				callback.setDefaultCredentialsProvider(credentialsProvider);
			} else {
				callback.disableAuthCaching();
			}
			return callback;
		});
		client = new RestHighLevelClient(builder);
		// sync config
		syncDbs = new HashSet<>(Arrays.asList(StringUtils.split(esProperties.getProperty("syncDbs", ""), ",")));
		String[] syncTables = esProperties.getProperty("syncTables", ".*").split(",");
		syncTablePatterns = new Pattern[syncTables.length];
		for (int i = 0; i < syncTables.length; i++) {
			syncTablePatterns[i] = Pattern.compile(syncTables[i]);
		}
		if (syncDbs.size() == 1 && syncDbs.contains("all")) {
			syncDbs.clear();
			syncDbs.addAll(this.getMysqlDbs());
		}
		logTables = new HashSet<>(Arrays.asList(StringUtils.split(esProperties.getProperty("logTables", ""), ",")));
		syncTableConfig = om.readValue(esProperties.getProperty("syncTableConfig", "{}"), new TypeReference<HashMap<String, ESTableConfig[]>>() {
		});
		batchLimit = Integer.parseInt(esProperties.getProperty("batchLimit", "1000"));
		batchTransactionLimit = Integer.parseInt(esProperties.getProperty("batchTransactionLimit", "20000"));
		numberOfShards = Integer.parseInt(esProperties.getProperty("numberOfShards", "1"));
		numberOfReplicas = Integer.parseInt(esProperties.getProperty("numberOfReplicas", "1"));
		maxResultWindow = Integer.parseInt(esProperties.getProperty("maxResultWindow", "1000000"));
		indexSettings = om.readValue(esProperties.getProperty("indexSettings", "{}"), HashMap.class);
		// init data
		initSchemas = "true".equalsIgnoreCase(esProperties.getProperty("initSchemas"));
		initData = "true".equalsIgnoreCase(esProperties.getProperty("initData"));
		initDataLock = "true".equalsIgnoreCase(esProperties.getProperty("initDataLock", "true"));
		initDataThreadNum = Integer.parseInt(esProperties.getProperty("initDataThreadNum", "10"));
		if (initSchemas) {
			this.initTable();
		}
		context.getMetrics().register(MetricRegistry.name(context.getConfig().metricsPrefix), new BinlogDelayGaugeSet(context));
	}


	@Override
	public synchronized void push(RowMap r) throws Exception {
		if (initSchemas && initData) {
			if (initPosition != null) {
				context.setPosition(initPosition);
			}
			LOG.info("InitSchemas completed!!! The program will exit!!! please set config initSchemas=false and restart,initPosition={}", context.getPosition());
			System.exit(0);
		}
		if (reqList.isEmpty() && r instanceof HeartbeatRowMap) {
			this.context.setPosition(r);
			return;
		}
		Long now = System.currentTimeMillis();
		if (now - lastUpdate > 1000) {
			this.batchUpdate(reqList);
		}
		if (!r.shouldOutput(outputConfig) || !isTableMatch(r.getDatabase(), r.getTable())) {
			return;
		}
		if (logTables.contains(r.getTable())) {
			LOG.info("push:{}", toJSON(r));
		}
		switch (r.getRowType()) {
			case "insert":
			case "bootstrap-insert":
			case "update":
			case "delete":
				this.reqDml(r);
				break;
			case "table-create":
			case "table-alter":
			case "ddl":
				LOG.info("ddl:" + this.toJSON(r));
				this.batchUpdate(reqList);
				this.syncTable(r.getDatabase(), r.getTable());
				break;
			case "bootstrap-start":
			case "bootstrap-complete":
				LOG.info("bootstrap:" + this.toJSON(r));
				break;
			default:
				this.batchUpdate(reqList);
				LOG.warn("unrecognizable type:{}", toJSON(r));
				break;
		}
		if (reqList.size() >= batchLimit && reqList.getLast().getRowMap().isTXCommit() || reqList.size() >= batchTransactionLimit) {
			// need batch commit or reached max batch size
			this.batchUpdate(reqList);
		}
	}

	private boolean isTableMatch(String database, String table) {
		String fullName = database + "." + table;
		Boolean isMatch = tableMatchCache.get(fullName);
		if (isMatch != null) {
			return isMatch;
		}
		isMatch = false;
		if (syncDbs.contains(database)) {
			for (Pattern syncTablePattern : syncTablePatterns) {
				if (syncTablePattern.matcher(fullName).matches()) {
					isMatch = true;
					break;
				}
			}
		}
		tableMatchCache.put(fullName, isMatch);
		return isMatch;
	}

	private List<TableColumn> syncTable(String database, String table) throws IOException {
		String fullName = database + "." + table;
		ESTableConfig[] tableConfigs = syncTableConfig.get(fullName);
		Map<String, TableColumn> fieldMap = this.getMysqlFields(database, table);
		if (tableConfigs != null) {
			for (ESTableConfig config : tableConfigs) {
				this.syncTable(fieldMap, config);
			}
		} else {
			ESTableConfig config = new ESTableConfig();
			config.setTargetName(table);
			this.syncTable(fieldMap, config);
		}
		List<TableColumn> priFields = new ArrayList<>(2);
		for (TableColumn TableColumn : fieldMap.values()) {
			if (TableColumn.isPri()) {
				priFields.add(TableColumn);
			}
		}
		return priFields;
	}

	private void syncTable(Map<String, TableColumn> fieldMap, ESTableConfig config) throws IOException {
		config.setTargetName(this.getTableName(config.getTargetName()));
		// get properties
		Map<String, Object> properties = new HashMap<>();
		if (config.getSourceFields() == null) {
			for (TableColumn column : fieldMap.values()) {
				Map<String, String> type = this.getTypeProp(config, column);
				Optional.ofNullable(type).ifPresent(o -> properties.put(column.getColumnName(), o));
			}
		} else {
			String[] fields = config.getSourceFields();
			for (int i = 0; i < fields.length; i++) {
				TableColumn column = fieldMap.get(fields[i]);
				Map<String, String> type = this.getTypeProp(config, column);
				int finalI = i;
				Optional.ofNullable(type).ifPresent(o -> properties.put(config.getTargetFields()[finalI], o));
			}
		}
		Map<String, Object> mapping = ImmutableMap.of("properties", properties);
		// check exists and then update/create
		if (client.indices().exists(new GetIndexRequest(config.getTargetName()), RequestOptions.DEFAULT)) {
			GetIndexRequest getReq = new GetIndexRequest(config.getTargetName());
			GetIndexResponse getRes = client.indices().get(getReq, RequestOptions.DEFAULT);
			Map<String, Object> sourcMap = getRes.getMappings().get(config.getTargetName()).getSourceAsMap();
			Map<String, Object> sourceProperties = (Map<String, Object>) sourcMap.get("properties");
			if (sourceProperties != null) {
				for (String key : sourceProperties.keySet()) {
					properties.remove(key);
				}
			}
			if (!properties.isEmpty()) {
				PutMappingRequest putReq = new PutMappingRequest(config.getTargetName());
				putReq.source(mapping);
				client.indices().putMapping(putReq, RequestOptions.DEFAULT);
			}
		} else {
			CreateIndexRequest createIndexRequest = new CreateIndexRequest(config.getTargetName());
			if (!properties.isEmpty()) {
				createIndexRequest.mapping(mapping);
			}
			Settings.Builder settings = Settings.builder().loadFromMap(indexSettings);
			settings.put("index.number_of_shards", Optional.ofNullable(config.getNumberOfShards()).orElse(numberOfShards));
			settings.put("index.number_of_replicas", Optional.ofNullable(config.getNumberOfReplicas()).orElse(numberOfReplicas));
			settings.put("index.max_result_window", Optional.ofNullable(config.getMaxResultWindow()).orElse(maxResultWindow));
			createIndexRequest.settings(settings);
			client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
		}
	}

	private Map<String, String> getTypeProp(ESTableConfig config, TableColumn TableColumn) {
		if (config.getTypeMap() != null) {
			String type = config.getTypeMap().get(TableColumn.getColumnName());
			if (type != null) {
				return ImmutableMap.of("type", type);
			}
		}
		switch (TableColumn.getDataType()) {
			// string
			case "char":
			case "varchar":
			case "enum":
			case "set":
				return ImmutableMap.of("type", TableColumn.getStrLen() <= 256 ? "keyword" : "text");
			case "text":
			case "tinytext":
			case "mediumtext":
			case "longtext":
				return ImmutableMap.of("type", "text");
			// number
			case "tinyint":
			case "smallint":
			case "year":
				return ImmutableMap.of("type", "short");
			case "int":
			case "mediumint":
				return ImmutableMap.of("type", "integer");
			case "bigint":
			case "bit":
				return ImmutableMap.of("type", "long");
			case "double":
			case "float":
				return ImmutableMap.of("type", TableColumn.getDataType());
			case "decimal":
				return ImmutableMap.of("type", "scaled_float", "scaling_factor", TableColumn.getNumericScale() == null ? "1" : String.valueOf(Math.pow(10, TableColumn.getNumericScale())));
			// date
			case "datetime":
			case "timestamp":
				return ImmutableMap.of("type", "date", "format", DATE_TIME_FORMAT);
//			case "date":
//				return ImmutableMap.of("type", "date", "format", "yyyy-MM-dd");
//			case "time":
//				return ImmutableMap.of("type", "date", "format", "HH:mm:ss");
			default:
				return null;

		}
	}


	private void reqDml(RowMap r) throws Exception {
		if (r.getPrimaryKeyValues().isEmpty()) {
			return;
		}
		String fullName = r.getDatabase() + "." + r.getTable();
		ESTableConfig[] tableConfigs = syncTableConfig.get(fullName);
		if (tableConfigs != null) {
			for (ESTableConfig config : tableConfigs) {
				if (config.getSourceFields() == null) {
					this.simpleDml(r, config.getTargetName());
				} else {
					if (!reqList.isEmpty() && !reqList.getLast().getRowMap().getTable().equals(r.getTable())) {
						this.batchUpdate(reqList);
					}
					List<DocWriteRequest> updateList = this.getUpdateList(config, r);
					for (DocWriteRequest req : updateList) {
						reqList.add(new ESReq(r, req));
					}
				}
			}
		} else {
			this.simpleDml(r, r.getTable());
		}
	}

	private LinkedHashMap<String, Object> getOldKeyValues(RowMap r, List<String> keys) {
		LinkedHashMap<String, Object> oldData = r.getOldData();
		if (CollectionUtils.containsAny(oldData.keySet(), keys)) {
			LinkedHashMap<String, Object> ret = new LinkedHashMap<>(keys.size());
			for (String k : keys) {
				Object v = oldData.get(k);
				if (v == null) {
					v = r.getData(k);
				}
				ret.put(k, v);
			}
			return ret;
		}
		return null;
	}

	private void simpleDml(RowMap r, String tableName) {
		tableName = this.getTableName(tableName);
		Object id;
		if (r.getPrimaryKeyValues().size() == 1) {
			id = r.getPrimaryKeyValues().get(0);
		} else {
			id = StringUtils.join(r.getPrimaryKeyValues(), "_");
		}
		switch (r.getRowType()) {
			case "insert":
			case "bootstrap-insert": {
				IndexRequest req = new IndexRequest(tableName);
				req.id(id.toString());
				req.source(r.getData());
				reqList.add(new ESReq(r, req));
				break;
			}
			case "update": {
				LinkedHashMap<String, Object> oldKeyValues = this.getOldKeyValues(r, r.getPrimaryKeyColumns());
				if (oldKeyValues != null) {
					String oldId = StringUtils.join(oldKeyValues.values(), "_");
					reqList.add(new ESReq(r, new DeleteRequest(tableName, oldId)));
					IndexRequest req = new IndexRequest(tableName);
					req.id(id.toString());
					req.source(r.getData());
					reqList.add(new ESReq(r, req));
				} else {
					UpdateRequest req = new UpdateRequest(tableName, id.toString());
					req.doc(r.getData());
					reqList.add(new ESReq(r, req));
				}
				break;
			}
			case "delete": {
				DeleteRequest req = new DeleteRequest(tableName, id.toString());
				reqList.add(new ESReq(r, req));
				break;
			}
		}
	}

	private String getTableName(String name) {
		if (name.indexOf('_') > 0 || UPPER_CASE_MATCHER.matchesAllOf(name)) {
			return name.toLowerCase();
		} else {
			return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
		}
	}

	private List<DocWriteRequest> getUpdateList(ESTableConfig config, RowMap r) throws Exception {
		if ("delete".equals(r.getRowType())) {
			return this.getUpdateList(config, null, Arrays.asList(r.getData()), true);
		}
		LinkedHashMap<String, Object> oldKeyValues = this.getOldKeyValues(r, Arrays.asList(config.getSourceKeys()));
		if (oldKeyValues != null) {
			// delete old and add new
			List<DocWriteRequest> updateList = this.getUpdateList(config, null, Arrays.asList(oldKeyValues), true);
			updateList.addAll(this.getUpdateList(config, null, Arrays.asList(r.getData()), false));
			return updateList;
		} else {
			return this.getUpdateList(config, null, Arrays.asList(r.getData()), false);
		}
	}

	private List<DocWriteRequest> getUpdateList(ESTableConfig config, List<TableColumn> priKeys, List<Map<String, Object>> batchList, boolean del) throws Exception {
		String tableName = getTableName(config.getTargetName());
		if (config.getSourceFields() == null) {
			return this.getUpdateListSimple(tableName, priKeys, batchList);
		}
		List<DocWriteRequest> ret = new ArrayList<>();
		// query by condition
		MultiSearchRequest multiSearchReq = new MultiSearchRequest();
		List<Map<String, Object>> updateList = new ArrayList<>();
		for (Map<String, Object> item : batchList) {
			String[] keys = config.getSourceKeys();
			Map<String, Object> query = new HashMap<>(keys.length);
			for (int i = 0; i < keys.length; i++) {
				query.put(config.getTargetKeys()[i], item.get(keys[i]));
			}
			String[] fields = config.getSourceFields();
			Map<String, Object> data = new HashMap<>(fields.length);
			if (!del && config.getDelTagField() != null) {
				Object delFile = item.get(config.getDelTagField());
				if (delFile != null && (delFile.toString().equals("1") || delFile.toString().equalsIgnoreCase("true"))) {
					del = true;
				}
			}
			for (int i = 0; i < fields.length; i++) {
				data.put(config.getTargetFields()[i], del ? null : item.get(fields[i]));
			}
			multiSearchReq.add(this.getQueryReq(tableName, query));
			updateList.add(data);
		}
		MultiSearchResponse response = client.msearch(multiSearchReq, RequestOptions.DEFAULT);
		// get update req
		MultiSearchResponse.Item[] responseItems = response.getResponses();
		for (int i = 0; i < responseItems.length; i++) {
			MultiSearchResponse.Item item = responseItems[i];
			if (item.isFailure()) {
				throw item.getFailure();
			} else {
				for (SearchHit hit : item.getResponse().getHits()) {
					Map<String, Object> data = hit.getSourceAsMap();
					data.putAll(updateList.get(i));
					UpdateRequest req = new UpdateRequest(tableName, hit.getId());
					req.doc(data);
					ret.add(req);
				}
			}
		}
		return ret;
	}

	private List<DocWriteRequest> getUpdateListSimple(String tableName, List<TableColumn> priKeys, List<Map<String, Object>> batchList) {
		List<DocWriteRequest> ret = new ArrayList<>();
		for (Map<String, Object> item : batchList) {
			IndexRequest req = new IndexRequest(tableName);
			if (priKeys != null && priKeys.size() > 0) {
				StringBuilder id = new StringBuilder();
				for (TableColumn priKey : priKeys) {
					if (id.length() == 0) {
						id.append(item.get(priKey.getColumnName()));
					} else {
						id.append("_").append(item.get(priKey.getColumnName()));
					}
				}
				req.id(id.toString());
			}
			req.source(item);
			ret.add(req);
		}
		return ret;
	}

	private SearchRequest getQueryReq(String tableName, Map<String, Object> cond) {
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Map.Entry<String, Object> e : cond.entrySet()) {
			query.must(QueryBuilders.termQuery(e.getKey(), e.getValue()));
		}
		SearchRequest request = new SearchRequest();
		request.indices(tableName);
		SearchSourceBuilder source = new SearchSourceBuilder();
		source.query(query);
		source.size(100000);
		request.source(source);
		return request;
	}

	private void batchUpdate(Deque<ESReq> reqList) throws IOException {
		lastUpdate = System.currentTimeMillis();
		if (reqList.isEmpty()) {
			return;
		}
		BulkRequest bulkRequest = new BulkRequest();
		for (ESReq r : reqList) {
			bulkRequest.add(r.getReq());
		}
		this.doBulkRequest(bulkRequest, true);
		this.context.setPosition(reqList.getLast().getRowMap());
		reqList.clear();
	}

	private void doBulkRequest(BulkRequest bulkRequest, boolean flush) throws IOException {
		if (bulkRequest.requests().isEmpty()) {
			return;
		}
		bulkRequest.setRefreshPolicy(flush ? WriteRequest.RefreshPolicy.IMMEDIATE : WriteRequest.RefreshPolicy.NONE);
		long start = System.currentTimeMillis();
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		long time = System.currentTimeMillis() - start;
		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse itemRes : bulkResponse) {
				if (itemRes.isFailed()) {
					if (isMsgException(itemRes.getFailure().getCause(), "document missing")) {
						LOG.warn("doBulkRequest ignore, msg={}", itemRes.getFailure());
					} else {
						LOG.error("doBulkRequest error, msg={}", itemRes.getFailure());
						throw new RuntimeException("batchUpdate error");
					}
				}
			}
		} else {
			LOG.info("doBulkRequest ret={},size={},time={},result={},table={}", bulkResponse.status(), bulkRequest.requests().size(), time, bulkResponse.getItems().length == bulkRequest.requests().size(), bulkRequest.getIndices());
		}
	}

	private boolean isMsgException(Exception e, String msg) {
		Throwable cause = e;
		while (true) {
			if (cause == null) {
				break;
			}
			if (cause.getMessage() != null && cause.getMessage().contains(msg)) {
				return true;
			} else {
				cause = cause.getCause();
			}
		}
		return false;
	}

	public String toJSON(RowMap r) {
		try {
			return r.toJSON(outputConfig);
		} catch (Exception e) {
			LOG.error("toJSON error:{}", r, e);
		}
		return null;
	}

	public Collection<String> getMysqlDbs() {
		return mysqlJdbcTemplate.queryForList(SQL_MYSQL_DBS, String.class);
	}

	public List<String> getMysqlTables(String tableSchema) {
		return mysqlJdbcTemplate.queryForList(SQL_MYSQL_TABLE, String.class, tableSchema);
	}

	public Map<String, TableColumn> getMysqlFields(String tableSchema, String tableName) {
		List<TableColumn> list = mysqlJdbcTemplate.query(SQL_MYSQL_FIELD, BeanPropertyRowMapper.newInstance(TableColumn.class), tableSchema, tableName);
		Map<String, TableColumn> map = list.stream().collect(Collectors.toMap(TableColumn::getColumnName, Function.identity()));
		return map;
	}

	@Override
	public void requestStop() throws Exception {
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {

	}

	private void initTable() throws IOException {
		if (initData) {
			Connection replicationConnection = null;
			try {
				replicationConnection = mysqlJdbcTemplate.getDataSource().getConnection();
				this.initData(syncDbs, replicationConnection);
			} catch (SQLException e) {
				LOG.error("sql error", e);
				throw new RuntimeException(e);
			} finally {
				try {
					if (replicationConnection != null && !replicationConnection.isClosed()) {
						if (initDataLock) {
							this.executeWithConn(replicationConnection, "unlock tables;");
						}
						JdbcUtils.closeConnection(replicationConnection);
					}
				} catch (SQLException e) {
					LOG.error("close error", e);
				}
			}
		} else {
			for (String database : syncDbs) {
				LOG.info("syncDatabase start:{}", database);
				List<String> tables = this.getMysqlTables(database);
				for (String table : tables) {
					if (this.isTableMatch(database, table)) {
						this.syncTable(database, table);
					}
				}
				LOG.info("syncDatabase end:{}", database);
			}
		}
	}

	private void executeWithConn(Connection conn, String sql) throws SQLException {
		try (Statement s = conn.createStatement()) {
			s.execute(sql);
		}
	}

	private void initData(Set<String> syncDbs, Connection replicationConnection) throws SQLException {
		long start = System.currentTimeMillis();
		// flush tables;
		// flush tables with read lock;
		// show master status;
		if (initDataLock) {
			this.executeWithConn(replicationConnection, "flush tables;");
			this.executeWithConn(replicationConnection, "flush tables with read lock;");
		}
		initPosition = Position.capture(replicationConnection, context.getConfig().gtidMode);
		LOG.info("current position={}", initPosition);
		// set session transaction isolation level repeatable read;
		DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
		definition.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
		TransactionStatus status = mysqlTransactionManager.getTransaction(definition);
		// not start transaction with consistent snapshot
		boolean transactionStart = false;
		ThreadPoolExecutor executor = new ThreadPoolExecutor(initDataThreadNum, initDataThreadNum, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(initDataThreadNum), Executors.defaultThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
		Integer insertCount = 0;
		try {
			for (String database : syncDbs) {
				List<String> tables = this.getMysqlTables(database);
				for (String table : tables) {
					if (!transactionStart) {
						mysqlJdbcTemplate.execute(String.format("select 1 from `%s`.`%s` limit 1", database, table));
						if (initDataLock) {
							this.executeWithConn(replicationConnection, "unlock tables;");
						}
						JdbcUtils.closeConnection(replicationConnection);
						LOG.info("lockTableTime={}", System.currentTimeMillis() - start);
						transactionStart = true;
					}
					if (!isTableMatch(database, table)) {
						continue;
					}
					List<TableColumn> priKeys = this.syncTable(database, table);
					ESTableConfig[] tableConfigs = syncTableConfig.get(database + "." + table);
					if (!priKeys.isEmpty() && tableConfigs == null) {
						ESTableConfig tableConfig = new ESTableConfig();
						tableConfig.setTargetName(table);
						tableConfigs = new ESTableConfig[]{tableConfig};
						insertCount += this.initTableData(database, table, executor, priKeys, tableConfigs);
						LOG.info("insertCount={},time={}", insertCount, System.currentTimeMillis() - start);
					}
				}
			}
			waitFinish(executor);
			for (String database : syncDbs) {
				List<String> tables = this.getMysqlTables(database);
				for (String table : tables) {
					if (!isTableMatch(database, table)) {
						continue;
					}
					ESTableConfig[] tableConfigs = syncTableConfig.get(database + "." + table);
					List<TableColumn> priKeys = this.syncTable(database, table);
					if (!priKeys.isEmpty() && tableConfigs != null) {
						waitFinish(executor);
						for (ESTableConfig config : tableConfigs) {
							FlushRequest request = new FlushRequest(getTableName(config.getTargetName()));
							request.force(true);
							client.indices().flush(request, RequestOptions.DEFAULT);
						}
						insertCount += this.initTableData(database, table, executor, priKeys, tableConfigs);
						LOG.info("insertCount={},time={}", insertCount, System.currentTimeMillis() - start);
					}
				}
			}
			// commit;
			mysqlTransactionManager.commit(status);
			waitFinish(executor);
		} catch (Exception e) {
			mysqlTransactionManager.rollback(status);
			LOG.error("sync data error", e);
		} finally {
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		LOG.info("all finish. insertCount={},time={}", insertCount, System.currentTimeMillis() - start);
	}

	private void waitFinish(ThreadPoolExecutor executor) {
		while (executor.getTaskCount() != executor.getCompletedTaskCount()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private Integer initTableData(String database, String table, ThreadPoolExecutor executor, List<TableColumn> priKeys, ESTableConfig[] tableConfigs) {
		// query all by mysql cursor
		String querySql = String.format("select * from `%s`.`%s`", database, table);
		Integer count = mysqlJdbcTemplate.query(con -> {
			final PreparedStatement statement = con.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(Integer.MIN_VALUE);
			return statement;
		}, new MyResultSetExtractor(database, table, executor, priKeys, tableConfigs));
		return count;
	}

	public class MyResultSetExtractor implements ResultSetExtractor<Integer> {
		final String database;
		final String table;
		final List<TableColumn> priKeys;
		final ESTableConfig[] tableConfigs;
		final ThreadPoolExecutor executor;
		// if has data
		int rowCount = 0;
		int columnCount = 0;
		String[] columnNames = null;
		long start = System.currentTimeMillis();

		public MyResultSetExtractor(String database, String table, ThreadPoolExecutor executor, List<TableColumn> priKeys, ESTableConfig[] tableConfigs) {
			this.database = database;
			this.table = table;
			this.executor = executor;
			this.priKeys = priKeys;
			this.tableConfigs = tableConfigs;
		}

		public void asyncBatchInsert(final List<Map<String, Object>> batchList) {
			executor.execute(() -> {
				try {
					BulkRequest bulkRequest = new BulkRequest();
					for (ESTableConfig config : tableConfigs) {
						List<DocWriteRequest> updateList = getUpdateList(config, priKeys, batchList, false);
						updateList.forEach(bulkRequest::add);
					}
					doBulkRequest(bulkRequest, false);
				} catch (Exception e) {
					LOG.error("asyncBatchInsert error", e);
				}
			});
		}

		@Override
		public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
			List<Map<String, Object>> batchList = new ArrayList<>();
			while (rs.next()) {
				if (rowCount++ == 0) {
					ResultSetMetaData rsmd = rs.getMetaData();
					columnCount = rsmd.getColumnCount();
					columnNames = new String[columnCount];
					for (int i = 0; i < columnCount; i++) {
						columnNames[i] = JdbcUtils.lookupColumnName(rsmd, i + 1);
					}
				}
				Map<String, Object> data = new HashMap<>(columnCount);
				for (int i = 0; i < columnCount; i++) {
					Object value = JdbcUtils.getResultSetValue(rs, i + 1);
					if (value instanceof Boolean) {
						value = (Boolean) value ? 1 : 0;
					} else if (value instanceof java.util.Date) {
						value = new SimpleDateFormat(DATE_TIME_FORMAT).format(value);
					} else if (value instanceof LocalDateTime) {
						value = ((LocalDateTime) value).format(DateTimeFormatter.ofPattern(DATE_TIME_FORMAT));
					}
					data.put(columnNames[i], value);
				}
				batchList.add(data);
				if (batchList.size() >= batchLimit) {
					this.asyncBatchInsert(batchList);
					batchList = new ArrayList<>();
				}
			}
			if (batchList.size() > 0) {
				this.asyncBatchInsert(batchList);
			}
			LOG.info("batch init query completed,table={}.{},rowCount={},time={}", database, table, rowCount, System.currentTimeMillis() - start);
			return rowCount;
		}
	}
}
