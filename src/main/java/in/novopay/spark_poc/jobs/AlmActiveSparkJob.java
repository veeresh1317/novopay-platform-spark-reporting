/*
package in.novopay.spark_poc.jobs;

import in.novopay.spark_poc.config.DatabaseConfig;
import in.novopay.spark_poc.jobs.utils.CDCStreaming;
//import in.novopay.spark_poc.jobs.utils.KafkaToClickHouseStreaming;

import in.novopay.spark_poc.jobs.utils.SchemaInferenceUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.apache.spark.sql.functions.*;


@Service
public class AlmActiveSparkJob implements DataJob {
    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlmActiveSparkJob.class);

    @Override
    public void loadData() {
        try {
            CDCStreaming cdcStreaming = new CDCStreaming(spark, databaseConfig);


            cdcStreaming.listKafkaTopics();

            // Schema inference for each topic
            StructType accountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.account");
            StructType loanAccountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.loan_account");
            StructType loanProductSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.loan_product");
            StructType productSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.product");
            StructType productSchemeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.product_scheme");
            StructType accountInterestDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.account_interest_details");
            StructType assetCriteriaSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.asset_criteria_slabs");
            StructType assetClassificationSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.asset_classification_slabs");
            StructType officeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_actor.office");
            StructType customerSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_actor.customer");
            StructType loanAppSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_los.loan_app");
            StructType loanAppPslDataSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_los.loan_app_psl_data");
            StructType groupDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_los.group_details");
            StructType loanAccountDerivedFieldsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_accounting.loan_account_derived_fields");
            StructType codeMasterSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_masterdata.code_master");
            StructType codeMasterDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark3.mfi_masterdata.code_master_details");


            // Start streaming for each topic
            Dataset<Row> accountStream = cdcStreaming.startStream("spark3.mfi_accounting.account", accountSchema);
            Dataset<Row> loanAccountStream = cdcStreaming.startStream("spark3.mfi_accounting.loan_account", loanAccountSchema);
            Dataset<Row> loanProductStream = cdcStreaming.startStream("spark3.mfi_accounting.loan_product", loanProductSchema);
            Dataset<Row> productStream = cdcStreaming.startStream("spark3.mfi_accounting.product", productSchema);
            Dataset<Row> productSchemeStream = cdcStreaming.startStream("spark3.mfi_accounting.product_scheme", productSchemeSchema);
            Dataset<Row> accountInterestDetailsStream = cdcStreaming.startStream("spark3.mfi_accounting.account_interest_details", accountInterestDetailsSchema);
            Dataset<Row> assetCriteriaSlabsStream = cdcStreaming.startStream("spark3.mfi_accounting.asset_criteria_slabs", assetCriteriaSlabsSchema);
            Dataset<Row> assetClassificationSlabsStream = cdcStreaming.startStream("spark3.mfi_accounting.asset_classification_slabs", assetClassificationSlabsSchema);
            Dataset<Row> officeStream = cdcStreaming.startStream("spark3.mfi_actor.office", officeSchema);
            Dataset<Row> customerStream = cdcStreaming.startStream("spark3.mfi_actor.customer", customerSchema);
            Dataset<Row> loanAppStream = cdcStreaming.startStream("spark3.mfi_los.loan_app", loanAppSchema);
            Dataset<Row> loanAppPslDataStream = cdcStreaming.startStream("spark3.mfi_los.loan_app_psl_data", loanAppPslDataSchema);
            Dataset<Row> groupDetailsStream = cdcStreaming.startStream("spark3.mfi_los.group_details", groupDetailsSchema);
            Dataset<Row> loanAccountDerivedFieldsStream = cdcStreaming.startStream("spark3.mfi_accounting.loan_account_derived_fields", loanAccountDerivedFieldsSchema);
            Dataset<Row> codeMasterStream = cdcStreaming.startStream("spark3.mfi_masterdata.code_master", codeMasterSchema);
            Dataset<Row> codeMasterDetailsStream = cdcStreaming.startStream("spark3.mfi_masterdata.code_master_details", codeMasterDetailsSchema);



            logger.info("Schema for groupDetails: ");
            groupDetailsStream.printSchema();
            logger.info("Schema for codeMasterStream: ");
            codeMasterStream.printSchema();

            logger.info("account stream with ts_ms");
            accountStream.printSchema();

                // Extract repayment frequency details
    //            Dataset<Row> repaymentFrequencyMaster = codeMasterStream
    //                    .filter(col("payload.after.data_type.value").equalTo("REPAYMENT_FREQUENCY")
    //                            .and(col("payload.after.data_sub_type.value").equalTo("DEFAULT")))
    //                    .withColumn("group_weaker_section",
    //                            functions.get_json_object(col("payload.after.extensible_json_data.value"), "$.group_weaker_section"));
    //
    //// Create a Dataset for repayment frequency details
    //            Dataset<Row> repaymentFrequencyDetails = codeMasterDetailsStream
    //                    .join(repaymentFrequencyMaster, codeMasterDetailsStream.col("payload.after.code_master_id.value")
    //                            .equalTo(repaymentFrequencyMaster.col("payload.after.id.value")))
    //                    .select(codeMasterDetailsStream.col("payload.after.code.value"),
    //                            codeMasterDetailsStream.col("payload.after.value.value").alias("repayment_freq_value"));
    //
    //// Extract JSON fields from group details
    //            Dataset<Row> groupDetailsWithJsonFields = groupDetailsStream
    //                    .withColumn("group_weaker_section", functions.get_json_object(col("payload.after.extensible_json_data"), "$.group_weaker_section"))
    //                    .withColumn("group_psl", functions.get_json_object(col("payload.after.extensible_json_data"), "$.group_psl"));


            //Step 1: Define watermarked datasets
            Dataset<Row> loanAppStreamWithWatermark = loanAppStream
                    .withColumn("updated_on", col("payload.after.updated_on.value").cast("timestamp")) // Cast to TIMESTAMP
                    .withWatermark("updated_on", "5 minute") // Watermark for loanAppStream
                    .select(
                            col("payload.after.loan_account_number.value").alias("l_number"),
                            col("payload.after.loan_account_id.value").alias("l_account_id"),
                            col("payload.after.status.value").alias("l_status"),
                            col("updated_on").alias("l_updated_on"), // Include updated_on in the select clause
                            col("payload.after.is_deleted.value").alias("l_is_deleted")
                    );

            Dataset<Row> accountStreamWithWatermark = accountStream
                    .withColumn("updated_on", col("payload.after.updated_on.value").cast("timestamp")) // Cast to TIMESTAMP
                    .withWatermark("updated_on", "30 minute") // Watermark for accountStream
                    .select(
                            col("payload.after.account_number.value").alias("a_number"),
                            col("payload.after.id.value").alias("a_id"),
                            col("payload.after.status.value").alias("a_status"),
                            col("updated_on").alias("a_updated_on"), // Include updated_on in the select clause
                            col("payload.after.is_deleted.value").alias("a_is_deleted")
                    );

// Print schemas for debugging
            loanAppStreamWithWatermark.printSchema();
            accountStreamWithWatermark.printSchema();

// Step 2: Perform the left outer join with a time range condition
            Dataset<Row> joinedWatermarkStream = loanAppStreamWithWatermark
                    .join(
                            accountStreamWithWatermark,
                            expr(
                                    "l_number = a_number AND " +
                                            "l_updated_on >= a_updated_on - interval 1 minutes AND " +
                                            "l_updated_on <= a_updated_on + interval 1 minutes"
                            ),
                            "left_outer"
                    )
                    .select(
                            loanAppStreamWithWatermark.col("l_number").alias("loan_account_number"),
                            loanAppStreamWithWatermark.col("l_account_id").alias("loan_account_id"),
                            loanAppStreamWithWatermark.col("l_status").alias("loan_status"),
//                            loanAppStreamWithWatermark.col("l_updated_on").alias("loan_updated_on"), // Include updated_on in the result
                            loanAppStreamWithWatermark.col("l_is_deleted").alias("loan_is_deleted"),
                            accountStreamWithWatermark.col("a_number").alias("account_number"),
                            accountStreamWithWatermark.col("a_id").alias("account_id"),
                            accountStreamWithWatermark.col("a_status").alias("account_status"),
//                            accountStreamWithWatermark.col("a_updated_on").alias("account_updated_on"), // Include updated_on in the result
                            accountStreamWithWatermark.col("a_is_deleted").alias("account_is_deleted")
                    );

            //cdcStreaming.processAndSave(joinedWatermarkStream, "water_mark");



             //Perform joins on streams
            Dataset<Row> combinedStream = accountStream
                    .join(loanAccountStream, accountStream.col("payload.after.id.value").equalTo(loanAccountStream.col("payload.after.account_id.value"))
                            .and(loanAccountStream.col("payload.after.parent_loan_account_id.value").isNull()))
                    .join(loanProductStream, loanAccountStream.col("payload.after.loan_product_id.value").equalTo(loanProductStream.col("payload.after.id.value")))
                    .join(loanAccountDerivedFieldsStream, loanAccountDerivedFieldsStream.col("payload.after.acc_id.value")
                            .equalTo(loanAccountStream.col("payload.after.account_id.value")))
                    .join(productStream, loanProductStream.col("payload.after.product_id.value").equalTo(productStream.col("payload.after.id.value")))
                    .join(productSchemeStream, productStream.col("payload.after.id.value").equalTo(productSchemeStream.col("payload.after.product_id.value"))
                            .and(productSchemeStream.col("payload.after.is_deleted.value").equalTo(false)))
                    .join(accountInterestDetailsStream, accountStream.col("payload.after.id.value").equalTo(accountInterestDetailsStream.col("payload.after.account_id.value")))
                    .join(assetCriteriaSlabsStream, loanAccountStream.col("payload.after.asset_criteria_slabs_id.value").equalTo(assetCriteriaSlabsStream.col("payload.after.id.value"))
                            .and(assetCriteriaSlabsStream.col("payload.after.is_deleted.value").equalTo(false)))
                    .join(assetClassificationSlabsStream, loanAccountStream.col("payload.after.asset_classification_slabs_id.value").equalTo(assetClassificationSlabsStream.col("payload.after.id.value"))
                            .and(assetClassificationSlabsStream.col("payload.after.is_deleted.value").equalTo(false)))
                    .join(officeStream, accountStream.col("payload.after.office_id.value").equalTo(officeStream.col("payload.after.id.value"))
                            .and(officeStream.col("payload.after.is_deleted.value").equalTo(false)))
                    .join(customerStream, loanAccountStream.col("payload.after.customer_id.value").equalTo(customerStream.col("payload.after.id.value"))
                            .and(customerStream.col("payload.after.is_deleted.value").equalTo(false)))
                    //.join(joinedWatermarkStream, accountStream.col("payload.after.account_number.value").equalTo(joinedWatermarkStream.col("account_number")))
                    .select(
                            accountStream.col("payload.after.id.value").alias("id"),
                            accountStream.col("payload.after.account_number.value").alias("account_num"),
                            lit("B").alias("accrualbasis"),
                            loanAccountDerivedFieldsStream.col("payload.after.accrued_interest.value").alias("accured_interest"),
                            officeStream.col("payload.after.formatted_id.value").alias("branch"),
                            lit("INR").alias("currency_code"),
                            loanAccountDerivedFieldsStream.col("payload.after.total_pos.value").alias("current_balance"),
                            loanAccountDerivedFieldsStream.col("payload.after.next_installment_due_date.value").alias("due_date"),
                            accountInterestDetailsStream.col("payload.after.effective_rate.value").alias("interest_rate"),
                            productStream.col("payload.after.code.value").alias("loan_type"),
                            loanAccountStream.col("payload.after.maturity_date.value").alias("maturity_date"),
                            loanAccountStream.col("payload.after.loan_amount.value").alias("orig_balance"),
                            loanAccountStream.col("payload.after.term.value").alias("orig_term"),
                            loanAccountStream.col("payload.after.expected_disbursement_date.value").alias("origination_date"),
                            loanAccountDerivedFieldsStream.col("payload.after.next_emi_amount.value").alias("installment"),
                            lit("7").alias("pymt_type"),
                            when(accountInterestDetailsStream.col("payload.after.interest_rate_type.value").equalTo("FIXED"), "Fixed")
                                    .when(accountInterestDetailsStream.col("payload.after.interest_rate_type.value").equalTo("FLOATING"), "Floating")
                                    .otherwise(null).alias("rate_flag"),
                            when(accountInterestDetailsStream.col("payload.after.interest_rate_type.value").equalTo("FIXED"), "PLR1")
                                    .when(accountInterestDetailsStream.col("payload.after.interest_rate_type.value").equalTo("FLOATING"), "Base rate code")
                                    .otherwise(null).alias("reprice_index"),
                            loanAccountStream.col("payload.after.past_due_days.value").alias("dpd"),
                            concat(customerStream.col("payload.after.first_name.value"), lit(" "), customerStream.col("payload.after.middle_name.value"), lit(" "), customerStream.col("payload.after.last_name.value")).alias("customer_name"),
                            productSchemeStream.col("payload.after.id.value").alias("scheme_id"),
                            when(assetCriteriaSlabsStream.col("payload.after.criteria.value").equalTo("STANDARD"), "REGULAR")
                                    .when(assetCriteriaSlabsStream.col("payload.after.criteria.value").equalTo("NPA"), "NPA")
                                    .when(assetCriteriaSlabsStream.col("payload.after.criteria.value").equalTo("WRITE_OFF"), "WRITEOFF")
                                    .otherwise(null).alias("npa_staged_id"),
                            loanAccountDerivedFieldsStream.col("payload.after.inst_start_date.value").alias("inst_start_date"),
                            loanAccountDerivedFieldsStream.col("payload.after.total_pos.value").alias("current_book_balance"),
                            loanAccountStream.col("payload.after.first_repayment_date.value").alias("first_installment_date"),
                            lit("1").alias("instlnum"),
                            loanAccountDerivedFieldsStream.col("payload.after.paid_emi.value").alias("no_of_paid_inst"),
                            loanAccountDerivedFieldsStream.col("payload.after.last_payment_date.value").alias("last_inst_recd_date"),
                            when(loanProductStream.col("payload.after.loan_category.value").equalTo("LOAN_JLG"), "C")
                                    .when(loanProductStream.col("payload.after.loan_category.value").equalTo("LOAN_IND"), "I")
                                    .otherwise(null).alias("indv_corp_flag"),
                            when(loanProductStream.col("payload.after.loan_category.value").equalTo("LOAN_JLG"), "Corporate")
                                    .when(loanProductStream.col("payload.after.loan_category.value").equalTo("LOAN_IND"), "Individual")
                                    .otherwise(null).alias("customer_type"),
                            when(loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value").lt(lit(0)),
                                    loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value"))
                                    .otherwise(lit(0)).alias("ub_dr"),
                            when(loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value").gt(lit(0)),
                                    loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value"))
                                    .otherwise(lit(0)).alias("ub_cr"),
                            loanAccountDerivedFieldsStream.col("payload.after.loan_acc_billed_int_gl_balance_d.value").alias("oi_cr"),
                            loanAccountDerivedFieldsStream.col("payload.after.loan_acc_billed_int_gl_balance_c.value").alias("oi_dr"),
                            loanAccountDerivedFieldsStream.col("payload.after.loan_acc_billed_prin_gl_balance_d.value").alias("op_dr"),
                            loanAccountDerivedFieldsStream.col("payload.after.loan_acc_billed_prin_gl_balance_c.value").alias("op_cr"),
                            when(loanAccountDerivedFieldsStream.col("payload.after.suspense_amount.value").gt(lit(0)),
                                    loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value"))
                                    .otherwise(lit(0)).alias("is_dr"),
                            when(loanAccountDerivedFieldsStream.col("payload.after.suspense_amount.value").lt(lit(0)),
                                    loanAccountDerivedFieldsStream.col("payload.after.loan_acc_gl_bal.value"))
                                    .otherwise(lit(0)).alias("is_cr"),
                            when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("DOUB_1"), "DBT I")
                                    .when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("DOUB_2"), "DBT II")
                                    .when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("DOUB_3"), "DBT III")
                                    .when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("LOSS"), "LOSS")
                                    .when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("STD"), "STANDARD")
                                    .when(assetClassificationSlabsStream.col("payload.after.classification.value").equalTo("SUB_STD"), "SUBSTAND")
                                    .otherwise(null).alias("asset_class_id"),
                            customerStream.col("payload.after.formatted_id.value").alias("customer_id"),
                            when(loanProductStream.col("payload.after.loan_type.value").equalTo("COLLT_UNSEC"), "Un secured")
                                    .when(loanProductStream.col("payload.after.loan_type.value").equalTo("COLLT_SEC"), "Secured")
                                    .otherwise(null).alias("product_type"),
                            loanAccountDerivedFieldsStream.col("payload.after.suspense_gl_code.value").alias("suspense_gl_code"),
                            loanAccountDerivedFieldsStream.col("payload.after.account_gl_code.value").alias("account_gl_code"),
                            loanAccountDerivedFieldsStream.col("payload.after.billed_int_gl_code.value").alias("billed_int_gl_code"),
                            loanAccountDerivedFieldsStream.col("payload.after.billed_prin_gl_code.value").alias("billed_prin_gl_code")

                    )
                    .filter(accountStream.col("payload.after.status.value").equalTo("ACTIVE"));

//            Dataset<Row> combinedStreamWithTimestamp = combinedStream.withColumn("timestamp", functions.current_timestamp());


//            Dataset<Row> finalStreamData =combinedStreamWithTimestamp.join(joinedWatermarkStream, combinedStreamWithTimestamp.col("account_num").equalTo(joinedWatermarkStream.col("loan_account_number")));


            cdcStreaming.processAndSave(combinedStream, "alm_active_spark_job");




        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveData(Dataset<Row> data) {
        // Not required for this job
    }

    @Override
    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return processingLogic.apply(datasets);
    }
}
*/
