package in.novopay.spark_poc.jobs.utils;

import in.novopay.spark_poc.config.DatabaseConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;

public class CDCStreaming {
    private static final Logger logger = LoggerFactory.getLogger(CDCStreaming.class);

    private final SparkSession spark;
    private final DatabaseConfig databaseConfig;

    public CDCStreaming(SparkSession spark, DatabaseConfig databaseConfig) {
        this.spark = spark;
        this.databaseConfig = databaseConfig;
    }

    /**
     * Starts a streaming job for a given Kafka topic with schema inference.
     *
     * @param topic  Kafka topic to subscribe to.
     * @param schema Pre-inferred schema for the topic.
     * @return Dataset representing the streaming data.
     */
    public Dataset<Row> startStream(String topic, StructType schema) throws TimeoutException {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", databaseConfig.getKafkaServers())
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false") // Ignore data loss issues
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(org.apache.spark.sql.functions.from_json(col("json"), schema).as("data"))
                .select("data.*");
    }

    /**
     * Processes multiple streaming datasets and handles batch processing.
     *
     * @param combinedStream Dataset representing the joined and processed streams.
     * @param clickhouseTable Target ClickHouse table name.
     */
    public void processAndSave(Dataset<Row> combinedStream, String clickhouseTable) {
        try {
            StreamingQuery query = combinedStream.writeStream()
                    .foreachBatch((batch, batchId) -> {
                        logger.info("Processing batch ID is empty : {}", batch.isEmpty());
                        if (!batch.isEmpty()) {
//                            long recordCount = batch.count(); // Get the number of records
//                            logger.info("Processing batch ID: {}, Record Count: {}", batchId, recordCount);
                            saveToClickHouse(batch, clickhouseTable);
                        }
                    })
//                    .option("checkpointLocation", "/home/veeresh/Desktop/checkpoint/")
                    .option("failOnDataLoss", "false") // Ignore data loss issues
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .start();

            query.awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            logger.error("Error processing streams: {}", e.getMessage(), e);
        }
    }

    /**
     * Saves a batch of data to ClickHouse.
     *
     * @param batch           Dataset to save.
     * @param clickhouseTable Target ClickHouse table name.
     */
    private void saveToClickHouse(Dataset<Row> batch, String clickhouseTable) {
        try {
            logger.info("Saving data to ClickHouse table: {}", clickhouseTable);
            // Log the column names
            String[] columnNames = batch.columns();
            logger.info("Column names: {}", String.join(", ", columnNames));
            batch.write()
                    .mode("append")
                    .option("createTableOptions", "ENGINE = ReplacingMergeTree(account_id) ORDER BY account_id")
                    .jdbc(databaseConfig.getClickhouseUrl(), clickhouseTable, databaseConfig.getClickhouseProperties());
            logger.info("Data saved successfully to ClickHouse table: {}", clickhouseTable);
        } catch (Exception e) {
            logger.error("Error saving data to ClickHouse: {}", e.getMessage(), e);
        }
    }

    /**
     * Lists available Kafka topics.
     */
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


    public void writeStreamDirectlyToClickHouse(Dataset<Row> stream, String clickhouseTable) {
        try {
            StreamingQuery query = stream.writeStream()
                    .foreachBatch((batch, batchId) -> {
                        if (!batch.isEmpty()) {
                            logger.info("Writing batch {} to ClickHouse table: {}", batchId, clickhouseTable);
                            batch.write()
                                    .mode("append")
                                    .option("createTableOptions", "ENGINE = ReplacingMergeTree() ORDER BY id")
                                    .jdbc(databaseConfig.getClickhouseUrl(), clickhouseTable, databaseConfig.getClickhouseProperties());
                        }
                    })
                    .option("checkpointLocation", "/tmp/checkpoints/" + clickhouseTable)
                    .trigger(Trigger.ProcessingTime("1 minute"))
                    .start();

            query.awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            logger.error("Error while streaming to ClickHouse: {}", e.getMessage(), e);
        }
    }

}
