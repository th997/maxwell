package com.zendesk.maxwell.producer.es;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.C3P0ConnectionPool;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
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
import java.util.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * elasticsearch
 */
public class ESProducer extends AbstractProducer implements StoppableTask {
	private static final String SQL_GET_MYSQL_FIELD = "select column_name,column_type,data_type,column_comment,character_maximum_length str_len,numeric_precision,numeric_scale,column_default,is_nullable = 'YES' null_able,column_key = 'PRI' pri,extra ='auto_increment' auto_increment from information_schema.columns t where t.table_schema =? and table_name =?";
	private static final String SQL_MYSQL_DBS = "select schema_name from information_schema.schemata where schema_name not in ('information_schema','pg_catalog','public')";
	private static final String SQL_GET_MYSQL_TABLE = "select table_name from information_schema.tables where table_type != 'VIEW' and table_schema =?";
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
	private Pattern[] syncTablePatterns;
	private Map<String, ESTableConfig[]> syncTableConfig;
	private Integer numberOfShards;
	private Integer numberOfReplicas;
	private Integer maxResultWindow;
	// init data
	private boolean initSchemas;
	private boolean initData;
	private boolean initDataLock;
	private Integer initDataThreadNum;
	private boolean initDataDelete;
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
		syncTableConfig = om.readValue(esProperties.getProperty("syncTableConfig", "{}"), new TypeReference<HashMap<String, ESTableConfig[]>>() {
		});
		batchLimit = Integer.parseInt(esProperties.getProperty("batchLimit", "1000"));
		batchTransactionLimit = Integer.parseInt(esProperties.getProperty("batchTransactionLimit", "20000"));
		numberOfShards = Integer.parseInt(esProperties.getProperty("numberOfShards", "1"));
		numberOfReplicas = Integer.parseInt(esProperties.getProperty("numberOfReplicas", "1"));
		maxResultWindow = Integer.parseInt(esProperties.getProperty("maxResultWindow", "1000000"));
		// init data
		initSchemas = "true".equalsIgnoreCase(esProperties.getProperty("initSchemas"));
		initData = "true".equalsIgnoreCase(esProperties.getProperty("initData"));
		initDataLock = "true".equalsIgnoreCase(esProperties.getProperty("initDataLock", "true"));
		initDataThreadNum = Integer.parseInt(esProperties.getProperty("initDataThreadNum", "10"));
		initDataDelete = "true".equalsIgnoreCase(esProperties.getProperty("initDataDelete"));
		if (initSchemas) {
			this.initTableData();
		}
	}


	@Override
	public void push(RowMap r) throws Exception {
		if (initSchemas) {
			synchronized (this) {
				if (initPosition != null) {
					context.setPosition(initPosition);
				}
				LOG.info("InitSchemas completed!!! The program will exit!!! please set config initSchemas=false and restart,initPosition={}", context.getPosition());
				System.exit(0);
			}
		}
		Long now = System.currentTimeMillis();
		if (now - lastUpdate > 1000 && reqList.size() > 0 && reqList.getLast().getRowMap().isTXCommit()) {
			this.batchUpdate(reqList);
		}
		if (!r.shouldOutput(outputConfig) || !isTableMatch(r.getDatabase(), r.getTable())) {
			return;
		}
		switch (r.getRowType()) {
			case "insert":
			case "update":
			case "delete":
				this.reqDml(r);
				break;
			case "table-create":
			case "table-alter":
			case "ddl":
				LOG.info("ddl:" + this.toJSON(r));
				this.syncTable(r.getDatabase(), r.getTable());
				this.batchUpdate(reqList);
				break;
			case "bootstrap-start":
			case "bootstrap-insert":
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
		if (!syncDbs.contains(database)) {
			return false;
		}
		boolean isMatch = false;
		String fullName = database + "." + table;
		for (int i = 0; i < syncTablePatterns.length; i++) {
			if (syncTablePatterns[i].matcher(fullName).matches()) {
				isMatch = true;
			}
		}
		return isMatch;
	}

	private List<TableField> syncTable(String database, String table) throws IOException {
		String fullName = database + "." + table;
		ESTableConfig[] tableConfigs = syncTableConfig.get(fullName);
		Map<String, TableField> fieldMap = this.getMysqlFields(database, table);
		if (tableConfigs != null) {
			for (ESTableConfig config : tableConfigs) {
				this.syncTable(fieldMap, config);
			}
		} else {
			ESTableConfig config = new ESTableConfig();
			config.setTargetName(table);
			this.syncTable(fieldMap, config);
		}
		List<TableField> priFields = new ArrayList<>(2);
		for (TableField tableField : fieldMap.values()) {
			if (tableField.isPri()) {
				priFields.add(tableField);
			}
		}
		return priFields;
	}

	private void syncTable(Map<String, TableField> fieldMap, ESTableConfig config) throws IOException {
		config.setTargetName(config.getTargetName().toLowerCase());
		// get properties
		Map<String, Object> properties = new HashMap<>();
		if (config.getSourceFields() == null) {
			for (TableField tableFiled : fieldMap.values()) {
				if (tableFiled.getDataType().endsWith("char")) {
					properties.put(tableFiled.getColumnName(), ImmutableMap.of("type", "keyword"));
				}
			}
		} else {
			String[] fields = config.getSourceFields();
			for (int i = 0; i < fields.length; i++) {
				TableField tableFiled = fieldMap.get(fields[i]);
				if (tableFiled.getDataType().endsWith("char")) {
					properties.put(config.getTargetFields()[i], ImmutableMap.of("type", "keyword"));
				}
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
			Settings.Builder settings = Settings.builder();
			settings.put("index.number_of_shards", Optional.ofNullable(config.getNumberOfShards()).orElse(numberOfShards));
			settings.put("index.number_of_replicas", Optional.ofNullable(config.getNumberOfReplicas()).orElse(numberOfReplicas));
			settings.put("index.max_result_window", Optional.ofNullable(config.getMaxResultWindow()).orElse(maxResultWindow));
			createIndexRequest.settings(settings);
			client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
		}
	}


	private void reqDml(RowMap r) throws IOException {
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
					String[] keys = config.getSourceKeys();
					Map<String, Object> query = new HashMap<>(keys.length);
					for (int i = 0; i < keys.length; i++) {
						query.put(config.getTargetKeys()[i], r.getData(keys[i]));
					}
					String[] fields = config.getSourceFields();
					Map<String, Object> filed = new HashMap<>(fields.length);
					for (int i = 0; i < keys.length; i++) {
						filed.put(config.getTargetFields()[i], r.getData(fields[i]));
					}
					List<Map<String, Object>> dataList = this.queryData(config.getTargetName(), query);
					for (Map<String, Object> item : dataList) {
						item.putAll(filed);
						UpdateRequest req = new UpdateRequest(config.getTargetName().toLowerCase(), (String) item.get("_id"));
						req.doc(item);
						reqList.add(new ESReq(r, req));
					}
				}
			}
		} else {
			this.simpleDml(r, r.getTable());
		}
	}

	private void simpleDml(RowMap r, String tableName) {
		if (CollectionUtils.isEmpty(r.getPrimaryKeyValues())) {
			return;
		}
		tableName = tableName.toLowerCase();
		Object id;
		if (r.getPrimaryKeyValues().size() == 1) {
			id = r.getPrimaryKeyValues().get(0);
		} else {
			id = StringUtils.join(r.getPrimaryKeyValues(), "_");
		}
		switch (r.getRowType()) {
			case "insert": {
				IndexRequest req = new IndexRequest(tableName);
				req.id(id.toString());
				req.source(r.getData());
				reqList.add(new ESReq(r, req));
				break;
			}
			case "update": {
				UpdateRequest req = new UpdateRequest(tableName, id.toString());
				req.doc(r.getData());
				reqList.add(new ESReq(r, req));
				break;
			}
			case "delete": {
				DeleteRequest req = new DeleteRequest(tableName, id.toString());
				reqList.add(new ESReq(r, req));
				break;
			}
		}
	}


	private List<Map<String, Object>> queryData(String tableName, Map<String, Object> cond) throws IOException {
		List<Map<String, Object>> ret = new ArrayList<>();
		BoolQueryBuilder query = QueryBuilders.boolQuery();
		for (Map.Entry<String, Object> e : cond.entrySet()) {
			query.must(QueryBuilders.termQuery(e.getKey(), e.getValue()));
		}
		SearchRequest request = new SearchRequest();
		request.indices(tableName);
		SearchSourceBuilder source = new SearchSourceBuilder();
		source.query(query);
		request.source(source);
		SearchResponse response = client.search(request, RequestOptions.DEFAULT);
		for (SearchHit hit : response.getHits().getHits()) {
			ret.add(hit.getSourceAsMap());
		}
		return ret;
	}

	private void batchUpdate(Deque<ESReq> reqList) throws IOException {
		if (reqList.isEmpty()) {
			return;
		}
		BulkRequest bulkRequest = new BulkRequest();
		for (ESReq r : reqList) {
			bulkRequest.add(r.getReq());
		}
		this.doBulkRequest(bulkRequest);
		this.context.setPosition(reqList.getLast().getRowMap());
		reqList.clear();
	}

	private void doBulkRequest(BulkRequest bulkRequest) throws IOException {
		bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		if (bulkResponse.hasFailures()) {
			for (BulkItemResponse itemRes : bulkResponse) {
				if (itemRes.isFailed()) {
					if (isMsgException(itemRes.getFailure().getCause(), "document missing")) {
						LOG.warn("batchUpdate ignore, msg={}", itemRes.getFailure());
					} else {
						LOG.error("batchUpdate error, msg={}", itemRes.getFailure());
						throw new RuntimeException("batchUpdate error");
					}
				}
			}
		} else {
			LOG.info("batchUpdate ret={},actual={},expected={}", bulkResponse.status(), bulkResponse.getItems().length, bulkRequest.requests().size());
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

	public List<String> getMysqlTables(String tableSchema) {
		return mysqlJdbcTemplate.queryForList(SQL_GET_MYSQL_TABLE, String.class, tableSchema);
	}

	public Collection<String> getMysqlDbs() {
		return mysqlJdbcTemplate.queryForList(SQL_MYSQL_DBS, String.class);
	}

	public Map<String, TableField> getMysqlFields(String tableSchema, String tableName) {
		List<TableField> list = mysqlJdbcTemplate.query(SQL_GET_MYSQL_FIELD, BeanPropertyRowMapper.newInstance(TableField.class), tableSchema, tableName);
		Map<String, TableField> map = list.stream().collect(Collectors.toMap(TableField::getColumnName, Function.identity()));
		return map;
	}

	@Override
	public void requestStop() throws Exception {

	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {

	}

	private void initTableData() throws IOException {
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
					this.syncTable(database, table);
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

	// Refer to  https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/MySQL/MaterializeMetadata.cpp
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
					List<TableField> priKeys = this.syncTable(database, table);
					String fullName = database + "." + table;
					ESTableConfig[] tableConfigs = syncTableConfig.get(fullName);
					if (tableConfigs == null) {
						insertCount += this.initTableData(database, table, priKeys, executor);
					}
				}
			}
			for (String database : syncDbs) {
				List<String> tables = this.getMysqlTables(database);
				for (String table : tables) {
					if (!isTableMatch(database, table)) {
						continue;
					}
					String fullName = database + "." + table;
					ESTableConfig[] tableConfigs = syncTableConfig.get(fullName);
					if (tableConfigs != null) {
						// TODO
					}
				}
			}
			// commit;
			mysqlTransactionManager.commit(status);
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
		LOG.info("insertCount={},time={}", insertCount, System.currentTimeMillis() - start);
	}

	private Integer initTableData(String database, String table, List<TableField> priKeys, ThreadPoolExecutor executor) {
		String querySql = String.format("select * from `%s`.`%s`", database, table);
		Integer count = mysqlJdbcTemplate.query(con -> {
			final PreparedStatement statement = con.prepareStatement(querySql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			statement.setFetchSize(Integer.MIN_VALUE);
			return statement;
		}, new MyResultSetExtractor(database, table, priKeys, executor));
		return count;
	}

	public class MyResultSetExtractor implements ResultSetExtractor<Integer> {
		final String database;
		final String table;
		final List<TableField> priKeys;
		final ThreadPoolExecutor executor;
		// if has data
		int rowCount = 0;
		int columnCount = 0;
		String[] columnNames = null;
		long start = System.currentTimeMillis();

		public MyResultSetExtractor(String database, String table, List<TableField> priKeys, ThreadPoolExecutor executor) {
			this.database = database;
			this.table = table;
			this.executor = executor;
			this.priKeys = priKeys;
		}

		private void asyncBatchInsert(final List<Map<String, Object>> batchList) {
			executor.execute(() -> {
				long start = System.currentTimeMillis();
				BulkRequest bulkRequest = new BulkRequest();
				for (Map<String, Object> item : batchList) {
					IndexRequest req = new IndexRequest(table.toLowerCase());
					StringBuilder id = new StringBuilder();
					for (TableField priKey : priKeys) {
						if (id.length() == 0) {
							id.append(item.get(priKey.getColumnName()));
						} else {
							id.append("_" + item.get(priKey.getColumnName()));
						}
					}
					if (id.length() > 0) {
						req.id(id.toString());
					}
					req.source(item);
					bulkRequest.add(req);
				}
				try {
					doBulkRequest(bulkRequest);
				} catch (IOException e) {
					LOG.error("asyncBatchInsert error", e);
				}
				LOG.info("batch init insert,db={},table={},size={},time={}", database, table, batchList.size(), System.currentTimeMillis() - start);
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
					} else if (value instanceof Timestamp) {
						value = new Date(((Timestamp) value).getTime());
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
