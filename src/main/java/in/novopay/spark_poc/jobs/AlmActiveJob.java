package in.novopay.spark_poc.jobs;

import in.novopay.spark_poc.config.DatabaseConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.row_number;


@Service
public class AlmActiveJob implements DataJob {


    @Autowired
    private SparkSession spark;

    @Autowired
    private DatabaseConfig databaseConfig;

    @Override
    public void loadData() {
        Dataset<Row> account = loadTable("mfi_accounting.account");
        Dataset<Row> loanAccount = loadTable("mfi_accounting.loan_account");
        Dataset<Row> loanProduct = loadTable("mfi_accounting.loan_product");
        Dataset<Row> product = loadTable("mfi_accounting.product");
        Dataset<Row> productScheme = loadTable("mfi_accounting.product_scheme");
        Dataset<Row> accountInterestDetails = loadTable("mfi_accounting.account_interest_details");
        Dataset<Row> assetCriteriaSlabs = loadTable("mfi_accounting.asset_criteria_slabs");
        Dataset<Row> assetClassificationSlabs = loadTable("mfi_accounting.asset_classification_slabs");
        Dataset<Row> office = loadTable("mfi_actor.office");
        Dataset<Row> customer = loadTable("mfi_actor.customer");
        Dataset<Row> loanApp = loadTable("mfi_los.loan_app");
        Dataset<Row> loanAppPslData = loadTable("mfi_los.loan_app_psl_data");
        Dataset<Row> loanAccountDerivedFields = loadTable("mfi_accounting.loan_account_derived_fields");
        Dataset<Row> processedData = processData(account, loanAccount, loanProduct, product, productScheme, accountInterestDetails, assetCriteriaSlabs, assetClassificationSlabs, office, customer, loanApp, loanAppPslData, loanAccountDerivedFields);
        saveData(processedData);
    }

    @Override
    public Dataset<Row> processData(Dataset<Row> data) {
        return data;
    }

    @Override
    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails, Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields, Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails, Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer) {
        return null;
    }

    @Override
    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails, Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields, Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails, Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> loanAccountRestructuringDetails, Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer) {
        return null;
    }

    @Override
    public void saveData(Dataset<Row> data) {
        data.write()
                .mode(SaveMode.Append)
                .option("createTableOptions", "ENGINE = MergeTree() ORDER BY id")
                .jdbc(databaseConfig.getClickhouseUrl(), "alm_active_loan_view", databaseConfig.getClickhouseProperties());
    }

    private Dataset<Row> loadTable(String tableName) {
        return spark.read().jdbc(databaseConfig.getYugabyteUrl(), tableName, databaseConfig.getYugabyteProperties());
    }


    @Override
    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> productScheme, Dataset<Row> accountInterestDetails, Dataset<Row> assetCriteriaSlabs, Dataset<Row> assetClassificationSlabs, Dataset<Row> office, Dataset<Row> customer, Dataset<Row> loanApp, Dataset<Row> loanAppPslData, Dataset<Row> loanAccountDerivedFields) {
        return account.join(loanAccount, account.col("id").equalTo(loanAccount.col("account_id")))
                .join(loanProduct, loanAccount.col("loan_product_id").equalTo(loanProduct.col("id")))
                .join(product, loanProduct.col("product_id").equalTo(product.col("id")))
                .join(productScheme, product.col("id").equalTo(productScheme.col("product_id")).and(productScheme.col("is_deleted").equalTo(false)))
                .join(accountInterestDetails, account.col("id").equalTo(accountInterestDetails.col("account_id")))
                .join(assetCriteriaSlabs, loanAccount.col("asset_criteria_slabs_id").equalTo(assetCriteriaSlabs.col("id")).and(assetCriteriaSlabs.col("is_deleted").equalTo(false)))
                .join(assetClassificationSlabs, loanAccount.col("asset_classification_slabs_id").equalTo(assetClassificationSlabs.col("id")).and(assetClassificationSlabs.col("is_deleted").equalTo(false)))
                .join(office, account.col("office_id").equalTo(office.col("id")).and(office.col("is_deleted").equalTo(false)))
                .join(customer, loanAccount.col("customer_id").equalTo(customer.col("id")).and(customer.col("is_deleted").equalTo(false)))
                .join(loanApp, loanApp.col("loan_account_number").equalTo(account.col("account_number")).and(loanApp.col("is_deleted").equalTo(false)))
                .join(loanAppPslData, loanAppPslData.col("loan_app_id").equalTo(loanApp.col("id")).and(loanAppPslData.col("is_deleted").equalTo(false)), "left_outer")
                .join(loanAccountDerivedFields, loanAccountDerivedFields.col("acc_id").equalTo(loanAccount.col("account_id")), "left_outer")
                .select(
                        row_number().over(Window.partitionBy(lit(null).cast("string")).orderBy(lit(1))).alias("id"),
                        account.col("account_number").alias("account_num"),
                        lit("B").alias("accrualbasis"),
                        loanAccountDerivedFields.col("accrued_interest").alias("accured_interest"),
                        office.col("formatted_id").alias("branch"),
                        lit("INR").alias("currency_code"),
                        loanAccountDerivedFields.col("total_pos").alias("current_balance"),
                        loanAccountDerivedFields.col("next_installment_due_date").alias("due_date"),
                        // Add other select expressions following the same pattern...
                        customer.col("formatted_id").alias("customer_id")
                );
    }
}