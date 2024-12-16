//package in.novopay.spark_poc.jobs;
//
//import in.novopay.spark_poc.config.DatabaseConfig;
//import org.apache.hadoop.shaded.com.nimbusds.jose.shaded.json.JSONObject;
//import org.apache.spark.sql.*;
//import org.json.JSONArray;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.locks.ReentrantLock;
//import java.util.function.Function;
//
//import static org.apache.spark.sql.functions.expr;
//
//@Service
//public class GenericDataJob implements DataJob {
//
//    @Autowired
//    private SparkSession spark;
//
//    @Autowired
//    private DatabaseConfig databaseConfig;
//
//    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GenericDataJob.class);
//
//    private static final ReentrantLock refreshLock = new ReentrantLock();
//    private static final long REFRESH_INTERVAL_MS = 120000; // 2 minutes
//    private static long lastRefreshTime = 0;
//
//    private final Map<String, Dataset<Row>> tableMap = new HashMap<>();
//
//    @Override
//    public void loadData() {
//        try {
//            long startTime = System.currentTimeMillis();
//            logger.info("Starting data load and processing.");
//
//            JSONObject config = loadJsonConfig("config.json");
//
//            // Load tables dynamically
//            refreshDataIfRequired(config.getJSONArray("tables"));
//
//            // Perform joins dynamically
//            Dataset<Row> result = performJoins(config);
//
//            // Save result dynamically
//            saveData(result, config.getJSONObject("save"));
//
//            long endTime = System.currentTimeMillis();
//            logger.info("Data processing and saving completed in {} ms.", endTime - startTime);
//        } catch (Exception e) {
//            logger.error("Error in data load, process, or save operation.", e);
//        }
//    }
//
//    @Override
//    public void saveData(Dataset<Row> data) {
//
//    }
//
//    private JSONObject loadJsonConfig(String filePath) throws Exception {
//        String content = new String(Files.readAllBytes(Paths.get(filePath)));
//        return new JSONObject(content);
//    }
//
//    private void refreshDataIfRequired(JSONArray tables) {
//        if (System.currentTimeMillis() - lastRefreshTime > REFRESH_INTERVAL_MS) {
//            if (refreshLock.tryLock()) {
//                try {
//                    logger.info("Refreshing tables...");
//                    loadTables(tables);
//                    lastRefreshTime = System.currentTimeMillis();
//                } finally {
//                    refreshLock.unlock();
//                }
//            }
//        }
//    }
//
//    private void loadTables(JSONArray tables) {
//        for (int i = 0; i < tables.length(); i++) {
//            org.json.JSONObject tableConfig = tables.getJSONObject(i);
//            String tableName = tableConfig.getString("name");
//            String alias = tableConfig.getString("alias");
//            boolean cache = tableConfig.getBoolean("cache");
//
//            Dataset<Row> table = loadAndCacheTable(tableName, cache);
//            tableMap.put(alias, table);
//        }
//    }
//
//    private Dataset<Row> loadAndCacheTable(String tableName, boolean cache) {
//        try {
//            logger.info("Loading table: {}", tableName);
//            Dataset<Row> data = spark.read()
//                    .jdbc(databaseConfig.getYugabyteUrl(), tableName, databaseConfig.getYugabyteProperties());
//            if (cache) {
//                data = data.cache();
//            }
//            logger.info("Loaded {} with {} rows.", tableName, data.count());
//            return data;
//        } catch (Exception e) {
//            logger.error("Failed to load table: {}", tableName, e);
//            throw new RuntimeException("Failed to load table: " + tableName, e);
//        }
//    }
//
//    private Dataset<Row> performJoins(JSONObject config) {
//        JSONArray joins = config.getJSONArray("joins");
//        Dataset<Row> result = null;
//
//        for (int i = 0; i < joins.length(); i++) {
//            org.json.JSONObject joinConfig = joins.getJSONObject(i);
//            String leftTable = joinConfig.getString("leftTable");
//            String rightTable = joinConfig.getString("rightTable");
//            String joinType = joinConfig.getString("joinType");
//            JSONArray conditions = joinConfig.getJSONArray("conditions");
//
//            Dataset<Row> leftDataset = tableMap.get(leftTable);
//            Dataset<Row> rightDataset = tableMap.get(rightTable);
//
//            String joinCondition = String.join(" AND ", conditions.toList().toArray(new String[0]));
//            Column condition = expr(joinCondition);
//
//            result = result == null
//                    ? leftDataset.join(rightDataset, condition, joinType)
//                    : result.join(rightDataset, condition, joinType);
//        }
//
//        // Apply select transformations
//        JSONArray selectColumns = config.getJSONArray("select");
//        for (int i = 0; i < selectColumns.length(); i++) {
//            org.json.JSONObject columnConfig = selectColumns.getJSONObject(i);
//            String expression = columnConfig.getString("expression");
//            String alias = columnConfig.getString("alias");
//
//            result = result.withColumn(alias, expr(expression));
//        }
//
//        return result;
//    }
//
//    private void saveData(Dataset<Row> data, JSONObject saveConfig) {
//        try {
//            String table = saveConfig.getString("table");
//            String mode = saveConfig.getString("mode");
//            String engine = saveConfig.getString("engine");
//
//            logger.info("Saving data to ClickHouse table: {}", table);
//            data.write()
//                    .mode(SaveMode.valueOf(mode.toUpperCase()))
//                    .option("createTableOptions", "ENGINE = " + engine)
//                    .jdbc(databaseConfig.getClickhouseUrl(), table, databaseConfig.getClickhouseProperties());
//            logger.info("Data saved successfully to {}", table);
//        } catch (Exception e) {
//            logger.error("Error saving data to ClickHouse", e);
//        }
//    }
//
//    @Override
//    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
//        return null;
//    }
//}
