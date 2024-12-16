package in.novopay.spark_poc.jobs;

import in.novopay.spark_poc.config.DatabaseConfig;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.apache.spark.sql.functions.*;

@Service
public class AlmActiveJob implements DataJob {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AlmActiveJob.class);

    // Lock to manage synchronous refresh of data tables
    private static final ReentrantLock refreshLock = new ReentrantLock();

    // Last refresh timestamp to control data refresh intervals
    private static long lastRefreshTime = 0;

    // Define the refresh interval (15 minutes) to load fresh data
    private static final long REFRESH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(2);

    // Datasets for various database tables
    private Dataset<Row> account;
    private Dataset<Row> loanAccount;
    private Dataset<Row> loanProduct;
    private Dataset<Row> product;
    private Dataset<Row> productScheme;
    private Dataset<Row> accountInterestDetails;
    private Dataset<Row> assetCriteriaSlabs;
    private Dataset<Row> assetClassificationSlabs;
    private Dataset<Row> office;
    private Dataset<Row> customer;
    private Dataset<Row> loanApp;
    private Dataset<Row> loanAppPslData;
    private Dataset<Row> groupDetails;
    private Dataset<Row> loanAccountDerivedFields;
    private Dataset<Row> codeMaster;
    private Dataset<Row> codeMasterDetails;

    @Override
    public void loadData() {
        try {
            long startTime = System.currentTimeMillis();
            logger.info("Starting data load and processing.");

            // Ensure data is refreshed if needed or load data for the first time
            refreshDataIfRequired();

            // Join the tables after loading them
            Dataset<Row> result = joinTables();
            logger.info("Total rows after all joins: {}", result.count());

            long processTime = System.currentTimeMillis();
            logger.info("Data processing completed in {} ms.", processTime - startTime);

            // Save processed data
            saveData(result);

            long endTime = System.currentTimeMillis();
            logger.info("Data saved. Total execution time: {} ms.", endTime - startTime);

        } catch (Exception e) {
            logger.error("Error in data load, process, or save operation.", e);
        }
    }

    /**
     * Method to check if data needs to be refreshed and, if necessary, reloads the data.
     * The method uses a lock to ensure only one thread performs the refresh at a time.
     */
    private void refreshDataIfRequired() {
        if (System.currentTimeMillis() - lastRefreshTime > REFRESH_INTERVAL_MS) {
            // Attempt to acquire the lock for data refresh
            if (refreshLock.tryLock()) {
                try {
                    // Perform data table loading
                    logger.info("Started reloading data from YB");
                    loadDataTables();
                    lastRefreshTime = System.currentTimeMillis(); // Update the last refresh timestamp
                } finally {
                    refreshLock.unlock(); // Ensure lock is released
                }
            }
        }
    }

    /**
     * Loads all necessary tables and caches them to optimize performance.
     * Each table is loaded only once, and caching allows Spark to reuse it without reloading from the source.
     */
    private void loadDataTables() {
        account = loadAndCacheTable("mfi_accounting.account");
        loanAccount = loadAndCacheTable("mfi_accounting.loan_account");
        loanProduct = loadAndCacheTable("mfi_accounting.loan_product");
        product = loadAndCacheTable("mfi_accounting.product");
        productScheme = loadAndCacheTable("mfi_accounting.product_scheme");
        accountInterestDetails = loadAndCacheTable("mfi_accounting.account_interest_details");
        assetCriteriaSlabs = loadAndCacheTable("mfi_accounting.asset_criteria_slabs");
        assetClassificationSlabs = loadAndCacheTable("mfi_accounting.asset_classification_slabs");
        office = loadAndCacheTable("mfi_actor.office");
        customer = loadAndCacheTable("mfi_actor.customer");
        loanApp = loadAndCacheTable("mfi_los.loan_app");
        loanAppPslData = loadAndCacheTable("mfi_los.loan_app_psl_data");
        loanAccountDerivedFields = loadAndCacheTable("mfi_accounting.loan_account_derived_fields");
        groupDetails = loadAndCacheTable("mfi_los.group_details");
        codeMaster = loadAndCacheTable("mfi_masterdata.code_master");
        codeMasterDetails = loadAndCacheTable("mfi_masterdata.code_master_details");
    }

    /**
     * Loads a table from the database, caches the result, and logs the number of rows loaded.
     * This method is called for each required table to avoid redundant code.
     *
     * @param tableName Name of the table to be loaded.
     * @return Dataset<Row> containing the loaded table data.
     */
    private Dataset<Row> loadAndCacheTable(String tableName) {
        try {
            logger.info("Loading table: {}", tableName);
            Dataset<Row> data = spark.read()
                    .jdbc(databaseConfig.getYugabyteUrl(), tableName, databaseConfig.getYugabyteProperties())
                    .cache(); // Cache to optimize performance for subsequent operations
            logger.info("Loaded {} with {} rows.", tableName, data.count());
            return data;
        } catch (Exception e) {
            logger.error("Failed to load table: {}", tableName, e);
            throw new RuntimeException("Failed to load table: " + tableName, e);
        }
    }

    /**
     * Joins the necessary tables and applies required transformations.
     * Returns a Dataset<Row> with the final structure after all joins and transformations.
     */
    private Dataset<Row> joinTables() {
        // Example for extracting repayment frequency details from codeMaster
        Dataset<Row> repaymentFrequencyMaster = codeMaster
                .filter(col("data_type").equalTo("REPAYMENT_FREQUENCY")
                        .and(col("data_sub_type").equalTo("DEFAULT")));
        Dataset<Row> repaymentFrequencyDetails = codeMasterDetails
                .join(repaymentFrequencyMaster, codeMasterDetails.col("code_master_id").equalTo(repaymentFrequencyMaster.col("id")))
                .select(codeMasterDetails.col("code"), codeMasterDetails.col("value").alias("repayment_freq_value"));

        // Extract JSON fields from group details
        Dataset<Row> groupDetailsWithJsonFields = groupDetails
                .withColumn("group_weaker_section", get_json_object(col("extensible_json_data"), "$.group_weaker_section"))
                .withColumn("group_psl", get_json_object(col("extensible_json_data"), "$.group_psl"));

        // Perform joins and select necessary columns for final output
        return account.join(loanAccount, account.col("id").equalTo(loanAccount.col("account_id"))
                        .and(loanAccount.col("parent_loan_account_id").isNull()))
                .join(loanProduct, loanAccount.col("loan_product_id").equalTo(loanProduct.col("id")))
                .join(loanAccountDerivedFields, loanAccountDerivedFields.col("acc_id")
                        .equalTo(loanAccount.col("account_id")), "left_outer")
                .join(product, loanProduct.col("product_id").equalTo(product.col("id")))
                .join(productScheme, product.col("id").equalTo(productScheme.col("product_id"))
                        .and(productScheme.col("is_deleted").equalTo(false)))
                .join(accountInterestDetails, account.col("id").equalTo(accountInterestDetails.col("account_id")))
                .join(assetCriteriaSlabs, loanAccount.col("asset_criteria_slabs_id").equalTo(assetCriteriaSlabs.col("id"))
                        .and(assetCriteriaSlabs.col("is_deleted").equalTo(false)))
                .join(assetClassificationSlabs, loanAccount.col("asset_classification_slabs_id").equalTo(assetClassificationSlabs.col("id"))
                        .and(assetClassificationSlabs.col("is_deleted").equalTo(false)))
                .join(office, account.col("office_id").equalTo(office.col("id")).and(office.col("is_deleted").equalTo(false)))
                .join(customer, loanAccount.col("customer_id").equalTo(customer.col("id")).and(customer.col("is_deleted").equalTo(false)))
                .join(loanApp, loanApp.col("loan_account_number").equalTo(account.col("account_number"))
                        .and(loanApp.col("is_deleted").equalTo(false)), "left_outer")
                .join(loanAppPslData, loanAppPslData.col("loan_app_id").equalTo(loanApp.col("id"))
                        .and(loanAppPslData.col("is_deleted").equalTo(false)), "left_outer")
                .join(groupDetailsWithJsonFields, groupDetailsWithJsonFields.col("id").equalTo(loanAccount.col("filler_3")), "left_outer")
                .join(repaymentFrequencyDetails, loanAccount.col("repayment_frequency").equalTo(repaymentFrequencyDetails.col("code")), "left_outer")
                .select(
                       // row_number().over(Window.partitionBy(lit(null).cast("string")).orderBy(lit(1))).alias("id"),
                        account.col("id").alias("id"),
                        account.col("account_number").alias("account_num"),
                        lit("B").alias("accrualbasis"),
                        loanAccountDerivedFields.col("accrued_interest").alias("accured_interest"),
                        office.col("formatted_id").alias("branch"),
                        lit("INR").alias("currency_code"),
                        loanAccountDerivedFields.col("total_pos").alias("current_balance"),
                        loanAccountDerivedFields.col("next_installment_due_date").alias("due_date"),
                        repaymentFrequencyDetails.col("repayment_freq_value").alias("payment_frequency"),
                        accountInterestDetails.col("effective_rate").alias("interest_rate"),
                        product.col("code").alias("loan_type"),
                        loanAccount.col("maturity_date"),
                        loanAccount.col("loan_amount").alias("orig_balance"),
                        loanAccount.col("term").alias("orig_term"),
                        loanAccount.col("expected_disbursement_date").alias("origination_date"),
                        loanAccountDerivedFields.col("next_emi_amount").alias("installment"),
                        repaymentFrequencyDetails.col("repayment_freq_value").alias("intt_pymt_freq"),
                        lit("7").alias("pymt_type"),
                        getRateFlag(),
                        getRepriceIndex(),
                        loanAccount.col("past_due_days").alias("dpd"),
                        concat(customer.col("first_name"), lit(" "), customer.col("middle_name"), lit(" "), customer.col("last_name")).alias("customer_name"),
                        productScheme.col("id").alias("scheme_id"),
                        getWeakerSectionDesc(groupDetailsWithJsonFields),
                        getPsl(groupDetailsWithJsonFields),
                        getNpaStagedId(),
                        loanAccountDerivedFields.col("inst_start_date"),
                        loanAccountDerivedFields.col("total_pos").alias("current_book_balance"),
                        loanAccount.col("first_repayment_date").alias("first_installment_date"),
                        lit("1").alias("instlnum"),
                        loanAccountDerivedFields.col("paid_emi").alias("no_of_paid_inst"),
                        loanAccountDerivedFields.col("last_payment_date").alias("last_inst_recd_date"),
                        getIndividualCorpFlag(),
                        getCustomerType(),
                        getUbDr(),
                        getUbCr(),
                        loanAccountDerivedFields.col("loan_acc_billed_int_gl_balance_d").alias("oi_dr"),
                        loanAccountDerivedFields.col("loan_acc_billed_int_gl_balance_c").alias("oi_cr"),
                        loanAccountDerivedFields.col("loan_acc_billed_prin_gl_balance_d").alias("op_dr"),
                        loanAccountDerivedFields.col("loan_acc_billed_prin_gl_balance_c").alias("op_cr"),
                        getIsDr(),
                        getIsCr(),
                        getAssetClassId(),
                        customer.col("formatted_id").alias("customer_id"),
                        getProductType(),
                        loanAccountDerivedFields.col("suspense_gl_code"),
                        loanAccountDerivedFields.col("account_gl_code"),
                        loanAccountDerivedFields.col("billed_int_gl_code"),
                        loanAccountDerivedFields.col("billed_prin_gl_code"))
                .filter(account.col("status").equalTo("ACTIVE"));
    }

    /**
     * Saves the final processed data to ClickHouse database.
     * Data is saved with MergeTree engine in ClickHouse for efficient querying.
     *
     * @param data Final processed data to be saved
     */
    public void saveData(Dataset<Row> data) {
        try {
            logger.info("Saving data to ClickHouse");
            data.write()
                    .mode(SaveMode.Append)
                   // .option("createTableOptions", "ENGINE = MergeTree() ORDER BY id") // for insert 
                    .option("createTableOptions", "ENGINE = ReplacingMergeTree(id) ORDER BY id") // upsert
                    .jdbc(databaseConfig.getClickhouseUrl(), "alm_active_loan_view", databaseConfig.getClickhouseProperties());
            logger.info("Data saved successfully to alm_active_loan_view");
        } catch (Exception e) {
            logger.error("Error saving data to ClickHouse", e);
        }
    }

    @Override
    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return null;  // Not implemented for this use case
    }

    // Helper methods for complex column transformations
    private Column getRateFlag() {
        return when(accountInterestDetails.col("interest_rate_type").equalTo("FIXED"), "Fixed")
                .when(accountInterestDetails.col("interest_rate_type").equalTo("FLOATING"), "Floating")
                .otherwise(null).alias("rate_flag");
    }

    private Column getRepriceIndex() {
        return when(accountInterestDetails.col("interest_rate_type").equalTo("FIXED"), "PLR1")
                .when(accountInterestDetails.col("interest_rate_type").equalTo("FLOATING"), "Base rate code")
                .otherwise(null).alias("reprice_index");
    }

    private Column getWeakerSectionDesc(Dataset<Row> groupDetailsWithJsonFields) {
        return when(loanProduct.col("loan_category").isin("LOAN_JLG", "LOAN_IND"), loanAppPslData.col("weaker_section_desc"))
                .when(loanProduct.col("loan_category").equalTo("LOAN_SHG"), groupDetailsWithJsonFields.col("group_weaker_section"))
                .alias("weaker_section_desc");
    }

    private Column getPsl(Dataset<Row> groupDetailsWithJsonFields) {
        return when(loanProduct.col("loan_category").isin("LOAN_JLG", "LOAN_IND"), loanAppPslData.col("psl_category_code"))
                .when(loanProduct.col("loan_category").equalTo("LOAN_SHG"), groupDetailsWithJsonFields.col("group_psl"))  // Use col("group_psl") directly
                .alias("psl");
    }


    private Column getNpaStagedId() {
        return when(assetCriteriaSlabs.col("criteria").equalTo("STANDARD"), "REGULAR")
                .when(assetCriteriaSlabs.col("criteria").equalTo("NPA"), "NPA")
                .when(assetCriteriaSlabs.col("criteria").equalTo("WRITE_OFF"), "WRITEOFF")
                .otherwise(null).alias("npa_staged_id");
    }

    private Column getIndividualCorpFlag() {
        return when(loanProduct.col("loan_category").equalTo("LOAN_JLG"), "C")
                .when(loanProduct.col("loan_category").isin("LOAN_IND", "LOAN_SHG"), "I")
                .alias("indv_corp_flag");
    }

    private Column getCustomerType() {
        return when(loanProduct.col("loan_category").equalTo("LOAN_JLG"), "Corporate")
                .when(loanProduct.col("loan_category").isin("LOAN_IND", "LOAN_SHG"), "Individual")
                .alias("customer_type");
    }

    private Column getUbDr() {
        return when(loanAccountDerivedFields.col("loan_acc_gl_bal").lt(0), loanAccountDerivedFields.col("loan_acc_gl_bal"))
                .otherwise(lit(0)).alias("ub_dr");
    }

    private Column getUbCr() {
        return when(loanAccountDerivedFields.col("loan_acc_gl_bal").gt(0), loanAccountDerivedFields.col("loan_acc_gl_bal"))
                .otherwise(lit(0)).alias("ub_cr");
    }

    private Column getIsDr() {
        return when(loanAccountDerivedFields.col("suspense_amount").lt(0), loanAccountDerivedFields.col("suspense_amount"))
                .otherwise(lit(0)).alias("is_dr");
    }

    private Column getIsCr() {
        return when(loanAccountDerivedFields.col("suspense_amount").gt(0), loanAccountDerivedFields.col("suspense_amount"))
                .otherwise(lit(0)).alias("is_cr");
    }

    private Column getAssetClassId() {
        return when(assetClassificationSlabs.col("classification").equalTo("DOUB_1"), "DBT I")
                .when(assetClassificationSlabs.col("classification").equalTo("DOUB_2"), "DBT II")
                .when(assetClassificationSlabs.col("classification").equalTo("DOUB_3"), "DBT III")
                .when(assetClassificationSlabs.col("classification").equalTo("LOSS"), "LOSS")
                .when(assetClassificationSlabs.col("classification").equalTo("STD"), "STANDARD")
                .when(assetClassificationSlabs.col("classification").equalTo("SUB_STD"), "SUBSTAND")
                .alias("asset_class_id");
    }

    private Column getProductType() {
        return when(loanProduct.col("loan_type").equalTo("COLLT_UNSEC"), "Un secured")
                .when(loanProduct.col("loan_type").equalTo("COLLT_SEC"), "Secured")
                .alias("product_type");
    }


}
