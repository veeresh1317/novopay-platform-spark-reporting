package in.novopay.spark_poc.jobs.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SchemaInferenceUtil {
    private static final Logger logger = LoggerFactory.getLogger(SchemaInferenceUtil.class);

    private SchemaInferenceUtil() {
        // Utility class; prevent instantiation
    }

    public static StructType inferSchemaFromKafka(SparkSession spark, String kafkaBootstrapServers, String topic) {
        try {
            logger.info("Inferring schema for Kafka topic: {}", topic);

            Dataset<Row> kafkaData = spark.read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", kafkaBootstrapServers)
                    .option("subscribe", topic)
                    .option("startingOffsets", "earliest")
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
}
