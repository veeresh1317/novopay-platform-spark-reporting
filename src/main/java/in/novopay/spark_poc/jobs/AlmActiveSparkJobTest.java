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

import java.util.function.Function;

import static org.apache.spark.sql.functions.*;

@Service
public class AlmActiveSparkJobTest implements DataJob {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlmActiveSparkJobTest.class);

    @Override
    public void loadData() {
        try {
            CDCStreaming cdcStreaming = new CDCStreaming(spark, databaseConfig);

            // List Kafka topics (for debugging purposes)
            cdcStreaming.listKafkaTopics();

            // Define Kafka topics and infer schemas
            String[] topics = {
                    "spark.mfi_accounting.account",
                    "spark.mfi_accounting.loan_account",
                    "spark.mfi_accounting.loan_product",
                    "spark.mfi_accounting.product",
                    "spark.mfi_accounting.product_scheme",
                    "spark.mfi_accounting.account_interest_details",
                    "spark.mfi_accounting.asset_criteria_slabs",
                    "spark.mfi_accounting.asset_classification_slabs",
                    "spark.mfi_actor.office",
                    "spark.mfi_actor.customer",
                    "spark.mfi_los.loan_app",
                    "spark.mfi_los.loan_app_psl_data",
                    "spark.mfi_los.group_details",
                    "spark.mfi_accounting.loan_account_derived_fields",
                    "spark.mfi_masterdata.code_master",
                    "spark.mfi_masterdata.code_master_details"
            };

            // Infer schemas for each topic
            StructType accountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.account");
            StructType loanAccountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.loan_account");
            StructType loanProductSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.loan_product");
            StructType productSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.product");
            StructType productSchemeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.product_scheme");
            StructType accountInterestDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.account_interest_details");
            StructType assetCriteriaSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.asset_criteria_slabs");
            StructType assetClassificationSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.asset_classification_slabs");
            StructType officeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_actor.office");
            StructType customerSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_actor.customer");
            StructType loanAppSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_los.loan_app");
            StructType loanAppPslDataSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_los.loan_app_psl_data");
            StructType groupDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_los.group_details");
            StructType loanAccountDerivedFieldsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_accounting.loan_account_derived_fields");
            StructType codeMasterSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_masterdata.code_master");
            StructType codeMasterDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark.mfi_masterdata.code_master_details");

            // Start streaming for each topic with watermark
            Dataset<Row> accountStream = cdcStreaming.startStream("spark.mfi_accounting.account", accountSchema)
                    .alias("accountStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("account_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("account_id"),
                            col("payload.after.account_number.value").alias("account_num"),
                            col("payload.after.office_id.value").alias("account_office_id"),
                            col("payload.after.status.value").alias("status"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> loanAccountStream = cdcStreaming.startStream("spark.mfi_accounting.loan_account", loanAccountSchema)
                    .alias("loanAccountStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(
                            col("ts_ms_timestamp").alias("loan_account_ts_ms_timestamp"),
                            col("payload.after.account_id.value").alias("loan_account_id"),
                            col("payload.after.loan_product_id.value").alias("la_loan_product_id"),
                            col("payload.after.customer_id.value").alias("la_customer_id"),

                            // Null check for 'maturity_date'
                            when(col("payload.after.maturity_date.value").isNotNull()
                                            .and(col("payload.after.maturity_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.maturity_date.value")).notEqual(lit(""))),
                                    col("payload.after.maturity_date.value")
                                            .divide(1000000)  // Convert microseconds to seconds
                                            .cast("timestamp"))
                                    .otherwise(lit(null)).alias("maturity_date"),

                            col("payload.after.past_due_days.value").alias("dpd"),
                            col("payload.after.loan_amount.value").alias("loan_amount"),
                            col("payload.after.term.value").alias("term"),

                            // Null check for 'expected_disbursement_date'
                            when(col("payload.after.expected_disbursement_date.value").isNotNull()
                                            .and(col("payload.after.expected_disbursement_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.expected_disbursement_date.value")).notEqual(lit(""))),
                                    col("payload.after.expected_disbursement_date.value")
                                            .divide(1000000)  // Convert microseconds to seconds
                                            .cast("timestamp"))
                                    .otherwise(lit(null)).alias("expected_disbursement_date"),

                            // Null check for 'first_repayment_date'
                            when(col("payload.after.first_repayment_date.value").isNotNull()
                                            .and(col("payload.after.first_repayment_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.first_repayment_date.value")).notEqual(lit(""))),
                                    col("payload.after.first_repayment_date.value")
                                            .divide(1000000)
                                            .cast("timestamp"))
                                    .otherwise(lit(null)).alias("first_repayment_date"),

                            col("payload.after.asset_criteria_slabs_id.value").alias("la_asset_criteria_slabs_id"),
                            col("payload.after.asset_classification_slabs_id.value").alias("la_asset_classification_slabs_id"),
                            col("payload.after.filler_3.value").alias("la_filler_3")
                    );


            Dataset<Row> loanProductStream = cdcStreaming.startStream("spark.mfi_accounting.loan_product", loanProductSchema)
                    .alias("loanProductStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("loan_product_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("loan_product_id"),
                            col("payload.after.product_id.value").alias("lp_product_id"),
                            col("payload.after.repayment_frequency.value").alias("repayment_frequency"),
                            col("payload.after.loan_type.value").alias("loan_type"),
                            col("payload.after.loan_category.value").alias("loan_category"));



            Dataset<Row> productStream = cdcStreaming.startStream("spark.mfi_accounting.product", productSchema)
                    .alias("productStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("product_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("product_id"),
                            col("payload.after.code.value").alias("product_code"));

            Dataset<Row> productSchemeStream = cdcStreaming.startStream("spark.mfi_accounting.product_scheme", productSchemeSchema)
                    .alias("productSchemeStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("product_scheme_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("product_scheme_id"),
                            col("payload.after.product_id.value").alias("ps_product_id"),
                            col("payload.after.is_deleted.value").alias("ps_is_deleted"));

            Dataset<Row> accountInterestDetailsStream = cdcStreaming.startStream("spark.mfi_accounting.account_interest_details", accountInterestDetailsSchema)
                    .alias("accountInterestDetailsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("account_interest_details_ts_ms_timestamp"),
                            col("payload.after.account_id.value").alias("aid"),
                            col("payload.after.effective_rate.value").alias("interest_rate"),
                            col("payload.after.interest_rate_type.value").alias("interest_rate_type"));

            Dataset<Row> assetCriteriaSlabsStream = cdcStreaming.startStream("spark.mfi_accounting.asset_criteria_slabs", assetCriteriaSlabsSchema)
                    .alias("assetCriteriaSlabsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("asset_criteria_slabs_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("asset_criteria_slabs_id"),
                            col("payload.after.criteria.value").alias("criteria"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> assetClassificationSlabsStream = cdcStreaming.startStream("spark.mfi_accounting.asset_classification_slabs", assetClassificationSlabsSchema)
                    .alias("assetClassificationSlabsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("asset_classification_slabs_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("asset_classification_slabs_id"),
                            col("payload.after.classification.value").alias("classification"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> officeStream = cdcStreaming.startStream("spark.mfi_actor.office", officeSchema)
                    .alias("officeStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("office_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("office_id"),
                            col("payload.after.formatted_id.value").alias("office_formatted_id"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> customerStream = cdcStreaming.startStream("spark.mfi_actor.customer", customerSchema)
                    .alias("customerStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("customer_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("customer_id"),
                            col("payload.after.formatted_id.value").alias("customer_formatted_id"),
                            col("payload.after.first_name.value").alias("first_name"),
                            col("payload.after.middle_name.value").alias("middle_name"),
                            col("payload.after.last_name.value").alias("last_name"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> loanAppStream = cdcStreaming.startStream("spark.mfi_los.loan_app", loanAppSchema)
                    .alias("loanAppStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("loan_app_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("loan_app_id"),
                            col("payload.after.loan_account_number.value").alias("lapp_loan_account_number"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> loanAppPslDataStream = cdcStreaming.startStream("spark.mfi_los.loan_app_psl_data", loanAppPslDataSchema)
                    .alias("loanAppPslDataStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("loan_app_psl_data_ts_ms_timestamp"),
                            col("payload.after.loan_app_id.value").alias("psl_loan_app_id"),
                            col("payload.after.psl_category_code.value").alias("psl_category_code"),
                            col("payload.after.weaker_section_desc.value").alias("weaker_section_desc"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> groupDetailsStream = cdcStreaming.startStream("spark.mfi_los.group_details", groupDetailsSchema)
                    .alias("groupDetailsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "10 minutes")
                    .select(col("ts_ms_timestamp").alias("group_details_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("group_id"),
                            col("payload.after.is_deleted.value").alias("is_deleted"));

            Dataset<Row> loanAccountDerivedFieldsStream = cdcStreaming.startStream("spark.mfi_accounting.loan_account_derived_fields", loanAccountDerivedFieldsSchema)
                    .alias("loanAccountDerivedFieldsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(
                            col("ts_ms_timestamp").alias("loan_account_derived_fields_ts_ms_timestamp"),
                            col("payload.after.acc_id.value").alias("acc_id"),
                            col("payload.after.accrued_interest.value").alias("accrued_interest"),
                            col("payload.after.total_pos.value").alias("total_pos"),

                            // Null check for 'next_installment_due_date'
                            when(col("payload.after.next_installment_due_date.value").isNotNull()
                                            .and(col("payload.after.next_installment_due_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.next_installment_due_date.value")).notEqual(lit(""))),
                                    expr("date_add(to_date('1970-01-01'), cast(payload.after.next_installment_due_date.value as INT))"))
                                    .otherwise(lit(null)).alias("due_date"),

                            // Null check for 'inst_start_date'
                            when(col("payload.after.inst_start_date.value").isNotNull()
                                    .and(col("payload.after.inst_start_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.inst_start_date.value")).notEqual(lit(""))),
                                    expr("date_add(to_date('1970-01-01'), cast(payload.after.inst_start_date.value as INT))"))
                                    .otherwise(lit(null)).alias("inst_start_date"),

                            col("payload.after.next_emi_amount.value").alias("next_emi_amount"),
                            col("payload.after.loan_acc_gl_bal.value").alias("loan_acc_gl_bal"),
                            col("payload.after.loan_acc_billed_int_gl_balance_d.value").alias("loan_acc_billed_int_gl_balance_d"),
                            col("payload.after.loan_acc_billed_int_gl_balance_c.value").alias("loan_acc_billed_int_gl_balance_c"),
                            col("payload.after.loan_acc_billed_prin_gl_balance_d.value").alias("loan_acc_billed_prin_gl_balance_d"),
                            col("payload.after.loan_acc_billed_prin_gl_balance_c.value").alias("loan_acc_billed_prin_gl_balance_c"),
                            col("payload.after.suspense_amount.value").alias("suspense_amount"),
                            col("payload.after.paid_emi.value").alias("no_of_paid_inst"),

                            // Null check for 'last_payment_date'
                            when(col("payload.after.last_payment_date.value").isNotNull()
                                            .and(col("payload.after.last_payment_date.value").notEqual(lit(0)))
                                            .and(trim(col("payload.after.last_payment_date.value")).notEqual(lit(""))),
                                    col("payload.after.last_payment_date.value")
                                            .divide(1000000)  // Convert microseconds to seconds
                                            .cast("timestamp"))
                                    .otherwise(lit(null)).alias("last_payment_date"),
                            col("payload.after.suspense_gl_code.value").alias("suspense_gl_code"),
                            col("payload.after.account_gl_code.value").alias("account_gl_code"),
                            col("payload.after.billed_int_gl_code.value").alias("billed_int_gl_code"),
                            col("payload.after.billed_prin_gl_code.value").alias("billed_prin_gl_code")
                    );

/*               StreamingQuery query = loanAccountDerivedFieldsStream
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

            Dataset<Row> codeMasterStream = cdcStreaming.startStream("spark.mfi_masterdata.code_master", codeMasterSchema)
                    .alias("codeMasterStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("code_master_ts_ms_timestamp"),
                            col("payload.after.id.value").alias("code_master_id"),
                            col("payload.after.data_type.value").alias("data_type"),
                            col("payload.after.data_sub_type.value").alias("data_sub_type"));

            Dataset<Row> codeMasterDetailsStream = cdcStreaming.startStream("spark.mfi_masterdata.code_master_details", codeMasterDetailsSchema)
                    .alias("codeMasterDetailsStream")
                    .withColumn("ts_ms_timestamp", col("payload.ts_ms").cast("timestamp"))
                    .withWatermark("ts_ms_timestamp", "1 minutes")
                    .select(col("ts_ms_timestamp").alias("code_master_details_ts_ms_timestamp"),
                            col("payload.after.code_master_id.value").alias("code_master_details_id"),
                            col("payload.after.code.value").alias("code"),
                            col("payload.after.value.value").alias("value"));

            // To handle sub query
            // Step 1: Filter codeMasterStream to get relevant IDs
            Dataset<Row> filteredCodeMaster = codeMasterStream
                    .filter(
                            col("data_type").equalTo("REPAYMENT_FREQUENCY")
                                    .and(col("data_sub_type").equalTo("DEFAULT"))
                    )
                    .select(col("code_master_id").alias("filtered_code_master_id"));

            // Step 2: Join codeMasterDetailsStream with filteredCodeMaster to get required values
            Dataset<Row> repaymentFrequencySubquery = codeMasterDetailsStream
                    .join(
                            filteredCodeMaster,
                            col("code_master_details_id").equalTo(col("filtered_code_master_id")),
                            "inner"
                    )
                    .select(
                            col("code").alias("repayment_frequency_code"),
                            col("value").alias("intt_pymt_freq") ,
                            col("code_master_details_ts_ms_timestamp").alias("repayment_frequency_ts_ms_timestamp")
                    );

            // Step 3: Join with loanProductStream
            Dataset<Row> loanProductWithRepaymentFrequency = loanProductStream
                    .join(
                            repaymentFrequencySubquery,
                            expr("repayment_frequency = repayment_frequency_code"
                            )
                    )
                    .select(
                            loanProductStream.col("*"), // Keep all columns from loanProductStream
                            col("intt_pymt_freq") // Add the required column from the join
                    );


           //Step 4: Join all the streams
            Dataset<Row> combinedStream = accountStream
                    .join(
                            loanAccountStream,
                            expr(" loan_account_id = account_id")
                    )
                    .join(
                            loanProductWithRepaymentFrequency,
                            expr("loan_product_id = la_loan_product_id")
                    )
                   .join(
                            productStream,
                            expr("product_id = lp_product_id")
                    )
                    .join(
                          productSchemeStream,
                          expr("ps_product_id = product_id")
                  )
                    .join(
                           accountInterestDetailsStream,
                           expr("aid = account_id")
                   )
                   .join(
                           assetCriteriaSlabsStream,
                           expr("asset_criteria_slabs_id = la_asset_criteria_slabs_id")
                   )
                    .join(
                            assetClassificationSlabsStream,
                            expr("asset_classification_slabs_id = la_asset_classification_slabs_id")
                    )
                  .join(
                            officeStream,
                            expr("office_id = account_office_id")
                    )
                    .join(
                            customerStream,
                            expr("customer_id = la_customer_id")
                    )
                      .join(loanAppStream,
                           expr(
                                   "lapp_loan_account_number = account_num " +
                                           "AND loan_app_ts_ms_timestamp BETWEEN customer_ts_ms_timestamp - interval 5 second AND customer_ts_ms_timestamp + interval 80 minute"
                           ), "left_outer")
                   .join(loanAppPslDataStream,
                           expr(
                                   "psl_loan_app_id = loan_app_id " +
                                           "AND loan_app_psl_data_ts_ms_timestamp BETWEEN loan_app_ts_ms_timestamp - interval 5 second AND loan_app_ts_ms_timestamp + interval 80 minute"
                           ),"left_outer")
                   .join(loanAccountDerivedFieldsStream,
                           expr(
                                   "acc_id = loan_account_id " +
                                           "AND loan_account_derived_fields_ts_ms_timestamp BETWEEN loan_app_psl_data_ts_ms_timestamp - interval 5 second AND loan_app_psl_data_ts_ms_timestamp + interval 80 minute"
                           ),"left_outer")
                   .join(groupDetailsStream,
                           expr(
                                   "la_filler_3 = group_id " +
                                           "AND group_details_ts_ms_timestamp BETWEEN loan_account_derived_fields_ts_ms_timestamp - interval 5 second AND loan_account_derived_fields_ts_ms_timestamp + interval 80 minute"
                           ),"left_outer")

                    // Filter active accounts
                    .filter(col("status").equalTo("ACTIVE"))
                    // Select and alias the required columns
                    .select(
                            col("account_id"),
                            col("account_num"),
                            lit("B").alias("accrualbasis"),
                            col("accrued_interest"),
                            col("office_formatted_id").alias("branch"),
                            lit("INR").alias("currency_code"),
                            col("total_pos").alias("current_balance"),
                            col("due_date"),
//                                    .divide(1000000)  // Convert microseconds to seconds
//                                    .cast("timestamp")  // Cast the value to timestamp
//                                    .alias("due_date"),
//                            col("intt_pymt_freq").alias("pymt_freq"),
                            col("interest_rate"),
                            col("product_code").alias("loan_type"),
                            col("maturity_date"),
//                                    .divide(1000000)
//                                    .cast("timestamp")
//                                    .alias("maturity_date"),
                            col("loan_amount").alias("orig_balance"),
                            col("term").alias("orig_term"),
                            col("expected_disbursement_date"),
//                                    .divide(1000000)
//                                    .cast("timestamp")
//                                    .alias("origination_date"),
                            col("next_emi_amount").alias("installment"),
                            col("intt_pymt_freq").alias("intt_pymt_freq"),
                            lit("7").alias("pymt_type"),
                            when(col("interest_rate_type").equalTo("FIXED"), "Fixed")
                                    .when(col("interest_rate_type").equalTo("FLOATING"), "Floating")
                                    .otherwise(null).alias("rate_flag"),
                            when(col("interest_rate_type").equalTo("FIXED"), "PLR1")
                                    .when(col("interest_rate_type").equalTo("FLOATING"), "Base rate code")
                                    .otherwise(null).alias("reprice_index"),
                            col("dpd"),
                            concat_ws(" ",
                                    col("first_name"),
                                    col("middle_name"),
                                    col("last_name")
                            ).alias("customer_name"),
                            col("ps_product_id").alias("scheme_id"),
                            col("psl_category_code").alias("psl"),
                            when(col("criteria").equalTo("STANDARD"), "REGULAR")
                                    .when(col("criteria").equalTo("NPA"), "NPA")
                                    .when(col("criteria").equalTo("WRITE_OFF"), "WRITEOFF")
                                    .otherwise(null).alias("npa_staged_id"),
                            col("inst_start_date"),
                            col("weaker_section_desc"),
                            col("total_pos").alias("current_book_balance"),
                            col("first_repayment_date"),
//                                    .divide(1000000)
//                                    .cast("timestamp")
//                                    .alias("first_installment_date"),
                            lit("1").alias("instlnum"),
                            col("no_of_paid_inst"),
                            col("last_payment_date"),
//                                    .divide(1000000)
//                                    .cast("timestamp")
//                                    .alias("last_inst_recd_date"),
                            when(col("loan_category").equalTo("LOAN_JLG"), "C")
                                    .when(col("loan_category").equalTo("LOAN_IND"), "I")
                                    .otherwise(null).alias("indv_corp_flag"),
                            when(col("loan_category").equalTo("LOAN_JLG"), "Corporate")
                                    .when(col("loan_category").equalTo("LOAN_IND"), "Individual")
                                    .otherwise(null).alias("customer_type"),
                            when(col("loan_acc_gl_bal").lt(0), col("loan_acc_gl_bal"))
                                    .otherwise(0).alias("ub_dr"),
                            when(col("loan_acc_gl_bal").gt(0), col("loan_acc_gl_bal"))
                                    .otherwise(0).alias("ub_cr"),
                            col("loan_acc_billed_int_gl_balance_d").alias("oi_dr"),
                            col("loan_acc_billed_int_gl_balance_c").alias("oi_cr"),
                            col("loan_acc_billed_prin_gl_balance_d").alias("op_dr"),
                            col("loan_acc_billed_prin_gl_balance_c").alias("op_cr"),
                            when(col("suspense_amount").lt(0), col("suspense_amount"))
                                    .otherwise(0).alias("is_dr"),
                            when(col("suspense_amount").gt(0), col("suspense_amount"))
                                    .otherwise(0).alias("is_cr"),
                            when(col("classification").equalTo("DOUB_1"), "DBT I")
                                    .when(col("classification").equalTo("DOUB_2"), "DBT II")
                                    .when(col("classification").equalTo("DOUB_3"), "DBT III")
                                    .when(col("classification").equalTo("LOSS"), "LOSS")
                                    .when(col("classification").equalTo("STD"), "STANDARD")
                                    .when(col("classification").equalTo("SUB_STD"), "SUBSTAND")
                                    .otherwise(null).alias("asset_class_id"),
                            col("customer_formatted_id").alias("customer_id"),
                            when(col("loan_type").equalTo("COLLT_UNSEC"), "Un secured")
                                    .when(col("loan_type").equalTo("COLLT_SEC"), "Secured")
                                    .otherwise(null).alias("product_type"),
                            col("suspense_gl_code").alias("suspense_gl_code"),
                            col("account_gl_code").alias("account_gl_code"),
                            col("billed_int_gl_code").alias("billed_int_gl_code"),
                            col("billed_prin_gl_code").alias("billed_prin_gl_code")
                    );


            cdcStreaming.processAndSave(combinedStream, "alm_active");

/*
            StreamingQuery query = combinedStream
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

    }

    @Override
    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return null;
    }
}