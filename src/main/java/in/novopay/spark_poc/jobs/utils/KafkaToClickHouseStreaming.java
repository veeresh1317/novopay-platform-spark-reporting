//package in.novopay.spark_poc.jobs.utils;
//
//import in.novopay.spark_poc.config.DatabaseConfig;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.streaming.Trigger;
//import org.apache.spark.sql.types.StructType;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import java.util.concurrent.TimeoutException;
//
//import static in.novopay.spark_poc.jobs.utils.SchemaInferenceUtil.inferSchemaFromKafka;
//
//public class KafkaToClickHouseStreaming {
//
//    private SparkSession spark;
//    private final DatabaseConfig databaseConfig;
//    private CDCStreaming cdcStreaming; // Declare CDCStreaming here
//
//    public KafkaToClickHouseStreaming(SparkSession sparkSession, DatabaseConfig databaseConfig) {
//        this.spark = sparkSession;
//        this.databaseConfig = databaseConfig;
//        this.cdcStreaming = new CDCStreaming(sparkSession, databaseConfig); // Initialize it here
//    }
//
//    public void loadStreamToClickHouse() throws TimeoutException {
//        // List of Kafka topics for streaming
//        String[] topics = {
//                "spark2.mfi_accounting.account",
//                "spark2.mfi_accounting.loan_account",
//                "spark2.mfi_accounting.loan_product",
//                "spark2.mfi_accounting.product",
//                "spark2.mfi_accounting.product_scheme",
//                "spark2.mfi_accounting.account_interest_details",
//                "spark2.mfi_accounting.asset_criteria_slabs",
//                "spark2.mfi_accounting.asset_classification_slabs",
//                "spark2.mfi_actor.office",
//                "spark2.mfi_actor.customer",
//                "spark2.mfi_los.loan_app",
//                "spark2.mfi_los.loan_app_psl_data",
//                "spark2.mfi_los.group_details",
//                "spark2.mfi_accounting.loan_account_derived_fields",
//                "spark2.mfi_masterdata.code_master",
//                "spark2.mfi_masterdata.code_master_details"
//        };
//
//        // Create a map for topic schema inference
//        for (String topic : topics) {
//            StructType schema =inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), topic);
//            Dataset<Row> stream = cdcStreaming.startStream(topic, schema);
//            String clickhouseTable = topic.replace("spark2.", "").replace(".", "_") + "_clickhouse";
//            writeStreamToClickHouse(stream, clickhouseTable);
//        }
//    }
//
//
//
//
//}
