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
public class AlmActiveKafkaToClickHouse implements DataJob {
    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlmActiveKafkaToClickHouse.class);

    @Override
    public void loadData() {
        try {
            CDCStreaming cdcStreaming = new CDCStreaming(spark, databaseConfig);


            cdcStreaming.listKafkaTopics();

            // Schema inference for each topic
            StructType accountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.account");
//            StructType loanAccountSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.loan_account");
//            StructType loanProductSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.loan_product");
//            StructType productSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.product");
//            StructType productSchemeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.product_scheme");
//            StructType accountInterestDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.account_interest_details");
//            StructType assetCriteriaSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.asset_criteria_slabs");
//            StructType assetClassificationSlabsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.asset_classification_slabs");
//            StructType officeSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_actor.office");
//            StructType customerSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_actor.customer");
//            StructType loanAppSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_los.loan_app");
//            StructType loanAppPslDataSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_los.loan_app_psl_data");
//            StructType groupDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_los.group_details");
//            StructType loanAccountDerivedFieldsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_accounting.loan_account_derived_fields");
//            StructType codeMasterSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_masterdata.code_master");
//            StructType codeMasterDetailsSchema = SchemaInferenceUtil.inferSchemaFromKafka(spark, databaseConfig.getKafkaServers(), "spark2.mfi_masterdata.code_master_details");


            // Start streaming for each topic
            Dataset<Row> accountStream = cdcStreaming.startStream("spark2.mfi_accounting.account", accountSchema);
//            Dataset<Row> loanAccountStream = cdcStreaming.startStream("spark2.mfi_accounting.loan_account", loanAccountSchema);
//            Dataset<Row> loanProductStream = cdcStreaming.startStream("spark2.mfi_accounting.loan_product", loanProductSchema);
//            Dataset<Row> productStream = cdcStreaming.startStream("spark2.mfi_accounting.product", productSchema);
//            Dataset<Row> productSchemeStream = cdcStreaming.startStream("spark2.mfi_accounting.product_scheme", productSchemeSchema);
//            Dataset<Row> accountInterestDetailsStream = cdcStreaming.startStream("spark2.mfi_accounting.account_interest_details", accountInterestDetailsSchema);
//            Dataset<Row> assetCriteriaSlabsStream = cdcStreaming.startStream("spark2.mfi_accounting.asset_criteria_slabs", assetCriteriaSlabsSchema);
//            Dataset<Row> assetClassificationSlabsStream = cdcStreaming.startStream("spark2.mfi_accounting.asset_classification_slabs", assetClassificationSlabsSchema);
//            Dataset<Row> officeStream = cdcStreaming.startStream("spark2.mfi_actor.office", officeSchema);
//            Dataset<Row> customerStream = cdcStreaming.startStream("spark2.mfi_actor.customer", customerSchema);
//            Dataset<Row> loanAppStream = cdcStreaming.startStream("spark2.mfi_los.loan_app", loanAppSchema);
//            Dataset<Row> loanAppPslDataStream = cdcStreaming.startStream("spark2.mfi_los.loan_app_psl_data", loanAppPslDataSchema);
//            Dataset<Row> groupDetailsStream = cdcStreaming.startStream("spark2.mfi_los.group_details", groupDetailsSchema);
//            Dataset<Row> loanAccountDerivedFieldsStream = cdcStreaming.startStream("spark2.mfi_accounting.loan_account_derived_fields", loanAccountDerivedFieldsSchema);
//            Dataset<Row> codeMasterStream = cdcStreaming.startStream("spark2.mfi_masterdata.code_master", codeMasterSchema);
//            Dataset<Row> codeMasterDetailsStream = cdcStreaming.startStream("spark2.mfi_masterdata.code_master_details", codeMasterDetailsSchema);




            cdcStreaming.processAndSave(accountStream, "account_clickhouse");




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
