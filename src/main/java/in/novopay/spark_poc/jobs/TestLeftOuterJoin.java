package in.novopay.spark_poc.jobs;

import in.novopay.spark_poc.config.DatabaseConfig;
import in.novopay.spark_poc.jobs.utils.CDCStreaming;
import in.novopay.spark_poc.jobs.utils.SchemaInferenceUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

@Service
public class TestLeftOuterJoin implements DataJob {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestLeftOuterJoin.class);

    @Override
    public void loadData() {
        try {
            CDCStreaming cdcStreaming = new CDCStreaming(spark, databaseConfig);

            // List Kafka topics for debugging
            cdcStreaming.listKafkaTopics();

            // Define Kafka topics
            String accountTopic = "spark.mfi_accounting.account";
            String loanAccountTopic = "spark.mfi_accounting.loan_account";

            // Infer schemas for topics
            StructType accountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), accountTopic);
            StructType loanAccountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), loanAccountTopic);

            // Read account stream with increased watermark
            Dataset<Row> accountStream = cdcStreaming.startStream(accountTopic, accountSchema)
                    .alias("accountStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")  // Increased watermark duration
                    .select(
                            col("ts_ms_timestamp").alias("account_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("account_id"),
                            col("payload.after.account_number.value").alias("account_num"),
                            col("payload.after.office_id.value").alias("account_office_id"),
                            col("payload.after.status.value").alias("status"),
                            col("payload.after.is_deleted.value").alias("is_deleted")
                    );

            // Read loan account stream with increased watermark
            Dataset<Row> loanAccountStream = cdcStreaming.startStream(loanAccountTopic, loanAccountSchema)
                    .alias("loanAccountStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")  // Increased watermark duration
                    .select(
                            col("ts_ms_timestamp").alias("loan_account_ts_ms_timestamp"),
                            col("payload.after.account_id.value").alias("loan_account_id"),
                            col("payload.after.loan_product_id.value").alias("la_loan_product_id"),
                            col("payload.after.customer_id.value").alias("la_customer_id"),
                            col("payload.after.maturity_date.value").alias("maturity_date"),
                            col("payload.after.past_due_days.value").alias("dpd"),
                            col("payload.after.loan_amount.value").alias("loan_amount"),
                            col("payload.after.term.value").alias("term"),
                            col("payload.after.expected_disbursement_date.value").alias("expected_disbursement_date"),
                            col("payload.after.first_repayment_date.value").alias("first_repayment_date"),
                            col("payload.after.filler_3.value").alias("la_filler_3")
                    );

      /*      Dataset<Row> debugTimestamps = loanAccountStream
                    .select(col("loan_account_ts_ms_timestamp")).limit(5);

            Dataset<Row> debugTimestamps = accountStream
                    .select(col("account_ts_ms_timestamp")).limit(5);*/



           Dataset<Row> debugTimestamps = accountStream
                    .join(
                            loanAccountStream,
                            expr("account_id = loan_account_id " +
                                    "AND loan_account_ts_ms_timestamp " +
                                    "BETWEEN account_ts_ms_timestamp - INTERVAL 5 second " +
                                    "AND account_ts_ms_timestamp + INTERVAL 7 MINUTES"),
                            "left_outer"
                    )
                    .select(
//                            col("account_ts_ms_timestamp"),
//                            col("loan_account_ts_ms_timestamp"),
                            col("account_num"),
                            col("account_id"),
                            col("loan_account_id"),
                            col("loan_amount")
                            //expr("CAST(loan_account_ts_ms_timestamp AS timestamp) - CAST(account_ts_ms_timestamp AS timestamp)").alias("time_diff")

                    );

            cdcStreaming.processAndSave(debugTimestamps,"new_table" );


    /*        StreamingQuery query = debugTimestamps.writeStream()
                    .format("console")
                    .outputMode("append")
                    .option("truncate", "false") // Optional: to see full data
                    .start();

            query.awaitTermination();*/

       /*     Dataset<Row> combinedStream = accountStream
                    .join(
                            loanAccountStream,
                            expr(
                                    "account_id = loan_account_id " +
                                            "AND CAST(loan_account_ts_ms_timestamp AS timestamp) BETWEEN " +
                                            "CAST(account_ts_ms_timestamp AS timestamp) - INTERVAL 1 SECOND " +
                                            "AND CAST(account_ts_ms_timestamp AS timestamp) + INTERVAL 10 SECONDS"
                            ),
                            "left_outer"
                    )
                    .filter(col("status").equalTo("ACTIVE")) // Filter active accounts
                    .limit(10)
                    .select(
                            col("account_ts_ms_timestamp"),
                            col("loan_account_ts_ms_timestamp"),
                            col("account_num"),
                            col("account_id"),
                            expr("CAST(loan_account_ts_ms_timestamp AS timestamp) - CAST(account_ts_ms_timestamp AS timestamp)").alias("time_diff")
                    );*/

    /*        // Debugging output
            StreamingQuery query1 = combinedStream.writeStream()
                    .format("console")
                    .outputMode("append")
                    .option("truncate", "false") // Optional: to see full data
                    .start();

            // Keep stream running
            query1.awaitTermination();*/


         /*   StreamingQuery query = debugTimestamps
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .foreachBatch((batchDF, batchId) -> {
                        long count = batchDF.count();
                        if (count > 0) {
                            System.out.println("Batch " + batchId + " has " + count + " rows.");
                            batchDF.show(); // Optional: show batch data
                        } else {
                            System.out.println("Batch " + batchId + " is empty.");
                        }
                    })
                    .start();

            query.awaitTermination();*/

        } catch (Exception e) {
            logger.error("Error processing the job", e);
        }
    }

    @Override
    public void saveData(Dataset<Row> data) {
        // Not implemented in this example
    }

    @Override
    public Dataset<Row> processData(java.util.function.Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return null;
    }
}


/*
// inner join working
          Dataset<Row> combinedStream = accountStream
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
                            col("id").equalTo(col("account_id")), "left_outer"
                    ).select(
                            col("id"),
                            col("account_number"),
                            col("loan_amount")
                    ).limit(5);*/
