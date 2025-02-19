package in.novopay.spark_poc.jobs;
import in.novopay.spark_poc.config.DatabaseConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.sql.*;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

@Service
public class LoanAccountFlattenedEntityNew implements DataJob {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoanAccountFlattenedEntityNew.class);

    private Dataset<Row> accountStream;
    private Dataset<Row> loanAccountStream;
    private Dataset<Row> loanDisbursementModeDetailsStream;

    @Override
    public void loadData() {
        try {
            long startTime = System.currentTimeMillis();
            logger.info("Starting data load and processing with CDC.");

            // Step 1: List available Kafka topics
            listKafkaTopics();

            // Step 2: Start processing CDC streams from Kafka
            startCdcStreaming();

            logger.info("Data processing initialized in {} ms.", System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            logger.error("Error during data load: {}", e.getMessage(), e);
        }
    }

    // List available Kafka topics
    public void listKafkaTopics() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, databaseConfig.getKafkaServers());

        try (AdminClient adminClient = AdminClient.create(config)) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get();

            logger.info("Available Kafka topics: {}", topicNames);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to list Kafka topics: {}", e.getMessage(), e);
        }
    }

    private void startCdcStreaming() {
        try {
            // Step 1: Start reading from Kafka topics
            logger.info("Starting to read from Kafka topics.");
            accountStream = startStream("cdc6.mfi_accounting.account");
            loanAccountStream = startStream("cdc6.mfi_accounting.loan_account");
            loanDisbursementModeDetailsStream = startStream("cdc7.mfi_accounting.loan_disbursement_mode_details");

//            StructType beforeLdmd= new StructType()
//                    .add("id", StringType, true);
//
//            StructType beforeAccount= new StructType()
//                    .add("id", StringType, true);
//
//            StructType beforeLoanAccount= new StructType()
//                    .add("account_id", StringType, true);

            logger.info("Streams started successfully from Kafka topics.");

            // Step 2: Handling insert/update operations (when op is "c", "i", or "r")
            Dataset<Row> insertUpdateStream = accountStream
                    .filter(col("payload.op").isin("c", "i", "r")) // Filter for create, insert, or read operations
                    .select(
                            col("payload.op").alias("operation1"),
                            col("payload.after.id.value").alias("id"),
                            col("payload.after.account_number.value").alias("account_number")
                    )
                    .join(
                            loanAccountStream.select(
                                    col("payload.after.account_id.value").alias("account_id"),
                                    col("payload.after.loan_amount.value").alias("loan_amount"),
                                    col("payload.op").alias("operation2")
                            ),
                            col("id").equalTo(col("account_id"))
                    )
                    .join(
                            loanDisbursementModeDetailsStream.select(
                                    col("payload.after.loan_account_id.value").alias("loan_account_id"),
                                    col("payload.after.mode.value").alias("mode"),
                                    col("payload.op").alias("operation3")
                            ),
                            col("account_id").equalTo(col("loan_account_id"))
                    )
                    .select("id", "account_number", "loan_amount", "mode", "operation1", "operation2", "operation3");



            // Step 3: Handling delete operations (when op is "d")
//            Dataset<Row> deleteStream = accountStream
//                    .filter(col("payload.op").equalTo("d"))// Filter for delete operations
//                    .select(
//                            from_json(col("payload.before"), beforeAccount).alias("before_json"), // Parse the "before" payload as JSON
//                            col("before_json.id.value").alias("account_id")
//                    )
//                    .join(
//                            loanAccountStream
//                                    .select(
//                                            from_json(col("payload.before"), beforeLoanAccount).alias("before_json"), // Parse the "before" payload as JSON
//                                            col("before_json.account_id.value").alias("loan_account_id")
//                                    )
//                    .join(
//                            loanDisbursementModeDetailsStream.select(
//                                            from_json(col("payload.before"), beforeLdmd).alias("before_json"), // Parse the "before" payload as JSON
//                                            col("before_json.id.value").alias("ldmd_id")
//                                    )
//                    .select("account_id", "loan_account_id", "ldmd_id")));



            // Step 4: Process insert/update data
            StreamingQuery insertUpdateQuery = insertUpdateStream
                    .writeStream()
                    .foreachBatch((batch, batchId) -> {
                        logger.info("Processing insert/update data batch. Batch ID: {}", batchId);
                        if (!batch.isEmpty()) {
                            batch.show();  // Debugging purposes
                            saveToClickHouse(batch); // Handle insert/update operations
                        }
                    })
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .start();

/*//             Step 5: Process delete data
            StreamingQuery deleteQuery = deleteStream
                    .writeStream()
                    .foreachBatch((batch, batchId) -> {
                        logger.info("Processing delete data batch. Batch ID: {}", batchId);
                        if (!batch.isEmpty()) {
                            batch.show();  // Debugging purposes
                            saveToClickHouse(batch); // Handle delete operations
                        }
                    })
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .start();*/

            // Step 6: Graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("Stopping CDC streams gracefully.");
                    insertUpdateQuery.stop();
                    //deleteQuery.stop();
                } catch (Exception e) {
                    logger.error("Error during shutdown: {}", e.getMessage(), e);
                }
            }));

        } catch (TimeoutException e) {
            logger.error("Timeout while starting streams: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error during CDC streaming process: {}", e.getMessage(), e);
        }
    }


    // Method to start stream and apply inferred schema for the topic
    private Dataset<Row> startStream(String topic) throws TimeoutException, StreamingQueryException {
        StructType schema = inferSchemaFromKafka(topic);

        // Read data from Kafka
        Dataset<Row> rawStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", databaseConfig.getKafkaServers())
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                //.option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*");


        // Check if the stream is active (this will confirm that the stream is running and processing)
        if (rawStream.isStreaming()) {
            logger.info("Stream for topic '{}' is active.", topic);
        } else {
            logger.warn("Stream for topic '{}' is not active.", topic);
        }

        // Log the record count using foreachBatch within writeStream
        StreamingQuery query = rawStream.writeStream()
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batch, batchId) -> {
                    // Ensure there is data in the batch
                    long count = batch.count();
                    logger.info("Number of records in topic '{}' (Batch ID: {}): {}", topic, batchId, count);

                    if (count > 0) {
                        logger.info("Processing batch with ID: {}", batchId);
                        batch.show();  // Show the first few records for debugging
                        // Save to ClickHouse or perform any other operation here
                    } else {
                        logger.info("No records to process in batch: {}", batchId);
                    }
                })
                .trigger(Trigger.ProcessingTime("1 minute")) // Set trigger interval for streaming
                .start();

        // Wait for the termination of the stream
        //query.awaitTermination();

        return rawStream; // Return the dataset (although query is being processed separately)
    }

    // Method to infer schema from Kafka topic
    private StructType inferSchemaFromKafka(String topic) {
        try {
            logger.info("Inferring schema for Kafka topic: {}", topic);

            Dataset<Row> kafkaData = spark.read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", databaseConfig.getKafkaServers())
                    .option("subscribe", topic)
                    .option("startingOffsets", "earliest")
                    //.option("endingOffsets", "latest")
                    .load()
                    .selectExpr("CAST(value AS STRING) as json");

            // Sample data from Kafka for schema inference
            String sampleJson = kafkaData.limit(1).collectAsList().get(0).getString(0);

            // Use the sample data to infer schema
            Dataset<Row> jsonData = spark.read().json(spark.createDataset(List.of(sampleJson), Encoders.STRING()));
            StructType inferredSchema = jsonData.schema();

            logger.info("Inferred schema for topic {}: {}", topic, inferredSchema.treeString());
            return inferredSchema;
        } catch (Exception e) {
            logger.error("Failed to infer schema for topic: {}", topic, e);
            throw new RuntimeException("Failed to infer schema for topic: " + topic, e);
        }
    }

    // Method to save the data to ClickHouse
    // todo Need to handle Delete and refresh data for updates
 /*   private void saveToClickHouse(Dataset<Row> data) {
        try {
            logger.info("Saving data to ClickHouse.");
            data.write()
                    .mode(SaveMode.Append)
                    .option("createTableOptions", "ENGINE = ReplacingMergeTree(id) ORDER BY id")
                    .jdbc(databaseConfig.getClickhouseUrl(), "loan_flattened_table", databaseConfig.getClickhouseProperties());
            logger.info("Data saved successfully to ClickHouse.");
        } catch (Exception e) {
            logger.error("Error saving data to ClickHouse: {}", e.getMessage(), e);
        }
    }
*/

    private void saveToClickHouse(Dataset<Row> batch) {
        try {
            // Filter for deletes
//            Dataset<Row> deletes = batch.filter(col("operation1").equalTo("d")
//                    .or(col("operation2").equalTo("d"))
//                    .or(col("operation3").equalTo("d")));
//
//            if (!deletes.isEmpty()) {
//                logger.info("Processing delete records. Count: {}", deletes.count());
//                deletes.foreachPartition(rows -> {
//                    // Delete logic, e.g., issue DELETE SQL commands
//                    while (rows.hasNext()) {
//                        Row row = rows.next();
//                        // Use primary key to delete
//                        String id = row.getAs("id");
//                        deleteFromClickHouse(id);
//                    }
//                });
//            }

            // Filter for inserts/updates
            Dataset<Row> insertsUpdates = batch.filter(
                    col("operation1").notEqual("d")
                            .and(col("operation2").notEqual("d"))
                            .and(col("operation3").notEqual("d")));

            if (!insertsUpdates.isEmpty()) {
                logger.info("Processing insert/update records. Count: {}", insertsUpdates.count());
                insertsUpdates.write()
                        .mode(SaveMode.Append)
                        .option("createTableOptions", "ENGINE = ReplacingMergeTree(id) ORDER BY id")
                        .jdbc(databaseConfig.getClickhouseUrl(), "loan_flattened_table", databaseConfig.getClickhouseProperties());
            }
        } catch (Exception e) {
            logger.error("Error while saving to ClickHouse: {}", e.getMessage(), e);
        }
    }

    // Example delete logic using DatabaseConfig
//    public void deleteFromClickHouse(String id) {
//        try (Connection connection = DriverManager.getConnection(
//                databaseConfig.getClickhouseUrl(),
//                databaseConfig.getClickhouseProperties().getProperty("user"),
//                databaseConfig.getClickhouseProperties().getProperty("password"));
//             PreparedStatement stmt = connection.prepareStatement("DELETE FROM your_table_name WHERE id = ?")) {
//
//            stmt.setString(1, id);
//            stmt.executeUpdate();
//            logger.info("Record with ID {} deleted successfully.", id);
//        } catch (SQLException e) {
//            logger.error("Error while deleting record with ID {}: {}", id, e.getMessage(), e);
//        }
//    }

        @Override
    public void saveData(Dataset<Row> data) {
        // No direct implementation in this context.
    }

    @Override
    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return null;
    }
}