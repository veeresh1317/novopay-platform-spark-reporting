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
public class LoanAccountFlattenedEntity implements DataJob {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoanAccountFlattenedEntity.class);

    // Lock to manage synchronous refresh of data tables
    private static final ReentrantLock refreshLock = new ReentrantLock();

    // Last refresh timestamp to control data refresh intervals
    private static long lastRefreshTime = 0;

    // Define the refresh interval (15 minutes) to load fresh data
    private static final long REFRESH_INTERVAL_MS = TimeUnit.MINUTES.toMillis(1);

    // Datasets for various database tables
    private Dataset<Row> account;
    private Dataset<Row> loanAccount;
    private Dataset<Row> loanAccountDerivedFields;
    //private Dataset<Row> repaymentAccountDetails;
    private Dataset<Row> loanDisbursementModeDetails;

//    private Dataset<Row> productScheme;
//    private Dataset<Row> accountInterestDetails;
//    private Dataset<Row> assetCriteriaSlabs;
//    private Dataset<Row> assetClassificationSlabs;
//    private Dataset<Row> office;
//    private Dataset<Row> customer;
//    private Dataset<Row> loanApp;
//    private Dataset<Row> loanAppPslData;
//    private Dataset<Row> groupDetails;
//    private Dataset<Row> codeMaster;
//    private Dataset<Row> codeMasterDetails;

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
        loanAccountDerivedFields = loadAndCacheTable("mfi_accounting.loan_account_derived_fields");
        loanDisbursementModeDetails = loadAndCacheTable("mfi_accounting.loan_disbursement_mode_details");
       // repaymentAccountDetails = loadAndCacheTable("mfi_accounting.repayment_account_details");
//        productScheme = loadAndCacheTable("mfi_accounting.product_scheme");
//        accountInterestDetails = loadAndCacheTable("mfi_accounting.account_interest_details");
//        assetCriteriaSlabs = loadAndCacheTable("mfi_accounting.asset_criteria_slabs");
//        assetClassificationSlabs = loadAndCacheTable("mfi_accounting.asset_classification_slabs");
//        office = loadAndCacheTable("mfi_actor.office");
//        customer = loadAndCacheTable("mfi_actor.customer");
//        loanApp = loadAndCacheTable("mfi_los.loan_app");
//        loanAppPslData = loadAndCacheTable("mfi_los.loan_app_psl_data");
//        groupDetails = loadAndCacheTable("mfi_los.group_details");
//        codeMaster = loadAndCacheTable("mfi_masterdata.code_master");
//        codeMasterDetails = loadAndCacheTable("mfi_masterdata.code_master_details");
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
        // Perform joins and select necessary columns for final output
        return account.join(loanAccount, account.col("id").equalTo(loanAccount.col("account_id")))
//                .join(loanAccountDerivedFields, loanAccountDerivedFields.col("acc_id")
//                .equalTo(loanAccount.col("account_id")),"left_outer")
//                .join(repaymentAccountDetails, repaymentAccountDetails.col("account_number")
//                        .equalTo(account.col("account_number")), "left_outer")
                .join(loanDisbursementModeDetails, loanDisbursementModeDetails.col("loan_account_id")
                        .equalTo(loanAccount.col("account_id")))
                .select(
//                        account.col("*"),
//                        loanAccount.col("*"),
//                        loanProduct.col("*"),
//                        product.col("*")

                        account.col("id"),
                        account.col("account_number"),
                        loanAccount.col("loan_amount"),
                        //loanAccountDerivedFields.col("total_pos"),
                        loanDisbursementModeDetails.col("mode")
                        //repaymentAccountDetails.col("hold_amount")
                        //loanProduct.col("loan_category"),
                        //product.col("code")
                );
                //.filter(account.col("status").equalTo("ACTIVE"));
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
            data.show();
            data.write()
                    .mode(SaveMode.Append)
                   // .option("createTableOptions", "ENGINE = MergeTree() ORDER BY id") // for insert
                    .option("createTableOptions", "ENGINE = ReplacingMergeTree(id) ORDER BY id") // upsert
                    .jdbc(databaseConfig.getClickhouseUrl(), "loan_account_flattened_table1", databaseConfig.getClickhouseProperties());
            logger.info("Data saved successfully to loan_account_flattened table1.");
        } catch (Exception e) {
            logger.error("Error saving data to ClickHouse", e);
        }
    }

    @Override
    public Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets) {
        return null;  // Not implemented for this use case
    }

}
