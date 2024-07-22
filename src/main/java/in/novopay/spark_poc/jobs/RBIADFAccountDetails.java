//package in.novopay.spark_poc.jobs;
//
//import in.novopay.spark_poc.config.DatabaseConfig;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.SparkSession;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import static org.apache.spark.sql.functions.*;
//
//@Service
//public class RBIADFAccountDetails implements DataJob {
//
//    @Autowired
//    private SparkSession spark;
//
//    @Autowired
//    private DatabaseConfig databaseConfig;
//
//    @Override
//    public void loadData() {
//        Dataset<Row> loanProduct = loadTable("mfi_accounting.loan_product");
//        Dataset<Row> productTransactionCatalogue = loadTable("mfi_accounting.product__transaction_catalogue");
//        Dataset<Row> productTransactionCataloguePlaceholderIAD = loadTable("mfi_accounting.product_transaction_catalogue__placeholder__iad");
//        Dataset<Row> internalAccountDefinition = loadTable("mfi_accounting.internal_account_definition");
//        Dataset<Row> loanAccountInsuranceDetails = loadTable("mfi_accounting.loan_account_insurance_details");
//        Dataset<Row> loanAccountRestructuringDetails = loadTable("mfi_accounting.loan_account_restructuring_details");
//        Dataset<Row> loanAppCUMemberDetails = loadTable("mfi_los.loan_app__cu_member_details");
//        Dataset<Row> loanAppBETDetails = loadTable("mfi_los.loan_app__bet_details");
//        Dataset<Row> account = loadTable("mfi_accounting.account");
//        Dataset<Row> loanAccount = loadTable("mfi_accounting.loan_account");
//        Dataset<Row> product = loadTable("mfi_accounting.product");
//        Dataset<Row> accountInterestDetails = loadTable("mfi_accounting.account_interest_details");
//        Dataset<Row> loanRepaymentModeDetails = loadTable("mfi_accounting.loan_repayment_mode_details");
//        Dataset<Row> repaymentMandateDetails = loadTable("mfi_accounting.repayment_mandate_details");
//        Dataset<Row> repaymentAccountDetails = loadTable("mfi_accounting.repayment_account_details");
//        Dataset<Row> loanAccountDerivedFields = loadTable("mfi_accounting.loan_account_derived_fields");
//        Dataset<Row> office = loadTable("mfi_actor.office");
//        Dataset<Row> assetClassificationSlabs = loadTable("mfi_accounting.asset_classification_slabs");
//        Dataset<Row> assetCriteriaSlabs = loadTable("mfi_accounting.asset_criteria_slabs");
//        Dataset<Row> loanApp = loadTable("mfi_los.loan_app");
//        Dataset<Row> employee = loadTable("mfi_actor.employee");
//        Dataset<Row> actorContactDetailMapping = loadTable("mfi_actor.actor__contact_detail__mapping");
//        Dataset<Row> contactDetail = loadTable("mfi_actor.contact_detail");
//        Dataset<Row> user = loadTable("mfi_actor.user");
//        Dataset<Row> customer = loadTable("mfi_actor.customer");
//
//        Dataset<Row> glcodes = computeGLCodes(loanProduct, productTransactionCatalogue, productTransactionCataloguePlaceholderIAD, internalAccountDefinition);
//        Dataset<Row> lifeInsuranceDetails = computeLifeInsuranceDetails(loanAccountInsuranceDetails);
//        Dataset<Row> loanRestructuringDetails = computeLoanRestructuringDetails(loanAccountRestructuringDetails);
//        Dataset<Row> cuMemberDetails = computeCUMemberDetails(loanAppCUMemberDetails);
//        Dataset<Row> betDetails = computeBETDetails(loanAppBETDetails);
//
//        Dataset<Row> processedData = processData(account, loanAccount, loanProduct, product, accountInterestDetails, loanRepaymentModeDetails, repaymentMandateDetails,
//                repaymentAccountDetails, loanAccountDerivedFields, office, assetClassificationSlabs, assetCriteriaSlabs, loanApp,
//                lifeInsuranceDetails, employee, actorContactDetailMapping, contactDetail,loanAccountRestructuringDetails, loanRestructuringDetails, glcodes,
//                cuMemberDetails, betDetails, customer);
//
//        saveData(processedData);
//    }
//
//    @Override
//    public Dataset<Row> processData(Dataset<Row> data) {
//        return null;
//    }
//
//    @Override
//    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails, Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields, Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails, Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer) {
//        return null;
//    }
//
//    private Dataset<Row> loadTable(String tableName) {
//        return spark.read().jdbc(databaseConfig.getYugabyteUrl(), tableName, databaseConfig.getYugabyteProperties());
//    }
//
////    private Dataset<Row> computeGLCodes(Dataset<Row> loanProduct, Dataset<Row> productTransactionCatalogue, Dataset<Row> productTransactionCataloguePlaceholderIAD, Dataset<Row> internalAccountDefinition) {
////        return loanProduct.alias("lp_1")
////                .join(productTransactionCatalogue.alias("ptc"), col("ptc.product_id").equalTo(col("lp_1.product_id")))
////                .join(productTransactionCataloguePlaceholderIAD.alias("ptcpi"), col("ptcpi.product_transaction_catalogue_id").equalTo(col("ptc.id"))
////                        .and(col("ptcpi.placeholder_code").equalTo("LOAN_ACCOUNT")))
////                .join(internalAccountDefinition.alias("iad"), col("iad.id").equalTo(col("ptcpi.internal_account_definition_id")))
////                .groupBy("lp_1.product_id", "iad.general_ledger_code", "iad.description")
////                .agg(first("iad.general_ledger_code").alias("code"), first("iad.description").alias("description"))
////                .select("lp_1.product_id", "code", "description");
////    }
//
//
//    private Dataset<Row> computeGLCodes(Dataset<Row> loanProduct, Dataset<Row> productTransactionCatalogue, Dataset<Row> productTransactionCataloguePlaceholderIAD, Dataset<Row> internalAccountDefinition) {
//        return loanProduct.alias("lp_1")
//                .join(productTransactionCatalogue.alias("ptc"), col("ptc.product_id").equalTo(col("lp_1.product_id")))
//                .select(
//                        col("lp_1.product_id").alias("lp_product_id"),
//                        col("ptc.id").alias("ptc_id")
//                )
//                .join(productTransactionCataloguePlaceholderIAD.alias("ptcpi"), col("ptcpi.product_transaction_catalogue_id").equalTo(col("ptc_id"))
//                        .and(col("ptcpi.placeholder_code").equalTo("LOAN_ACCOUNT")))
//                .select(
//                        col("lp_product_id"),
//                        col("ptcpi.internal_account_definition_id").alias("ptcpi_iad_id")
//                )
//                .join(internalAccountDefinition.alias("iad"), col("iad.id").equalTo(col("ptcpi_iad_id")))
//                .select(
//                        col("lp_product_id"),
//                        col("iad.general_ledger_code").alias("iad_general_ledger_code"),
//                        col("iad.description").alias("iad_description")
//                )
//                .groupBy("lp_product_id", "iad_general_ledger_code", "iad_description")
//                .agg(
//                        first("iad_general_ledger_code").alias("code"),
//                        first("iad_description").alias("description")
//                )
//                .select(
//                        col("lp_product_id").alias("product_id"),
//                        col("code"),
//                        col("description")
//                );
//    }
//
//
//    private Dataset<Row> computeLifeInsuranceDetails(Dataset<Row> loanAccountInsuranceDetails) {
//        return loanAccountInsuranceDetails
//                .filter(col("policy_type").equalTo("LIFE_INSUR").and(col("is_deleted").equalTo(false)))
//                .groupBy("loan_account_id")
//                .agg(first("loan_account_id").alias("loan_account_id"));
//    }
//
//    private Dataset<Row> computeLoanRestructuringDetails(Dataset<Row> loanAccountRestructuringDetails) {
//        return loanAccountRestructuringDetails
//                .filter(col("is_deleted").equalTo(false)
//                        .and(col("restructuring_status").equalTo("SUCCESS"))
//                        .and(col("task_status").equalTo("APPROVED")))
//                .groupBy("loan_account_id")
//                .agg(min("id").alias("id"), min("rescheduling_effective_date").alias("restructuring_date"));
//    }
//
//    private Dataset<Row> computeCUMemberDetails(Dataset<Row> loanAppCUMemberDetails) {
//        return loanAppCUMemberDetails
//                .filter(col("is_deleted").equalTo(false))
//                .select("loan_app_id", "updated_by");
//    }
//
//    private Dataset<Row> computeBETDetails(Dataset<Row> loanAppBETDetails) {
//        return loanAppBETDetails
//                .filter(col("is_deleted").equalTo(false))
//                .select("loan_app_id", "updated_by");
//    }
//
//    @Override
//    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails,
//                                    Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields,
//                                    Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails,
//                                    Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> loanAccountRestructuringDetails,
//                                    Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer) {
//        return account.alias("a")
//                .join(loanAccount.alias("la"), col("la.account_id").equalTo(col("a.id")).and(col("la.is_deleted").equalTo(false)))
//                .join(loanProduct.alias("lp"), col("lp.id").equalTo(col("la.loan_product_id")))
//                .join(product.alias("p"), col("p.id").equalTo(col("la.loan_product_id")).and(col("p.is_deleted").equalTo(false)))
//                .join(accountInterestDetails.alias("aid"), col("aid.account_id").equalTo(col("la.account_id")))
//                .join(loanRepaymentModeDetails.alias("lrmd"), col("lrmd.loan_account_id").equalTo(col("la.account_id")), "left_outer")
//                .join(repaymentMandateDetails.alias("rmd"), col("rmd.loan_account_id").equalTo(col("la.account_id")).and(col("rmd.is_deleted").equalTo(false))
//                        .and(col("rmd.mandate_status").equalTo("ACTIVE")), "left_outer")
//                .join(repaymentAccountDetails.alias("rad"), col("rmd.repayment_account_details_id").equalTo(col("rad.id")).and(col("rad.is_deleted").equalTo(false)), "left_outer")
//                .join(loanAccountDerivedFields.alias("ladf"), col("ladf.acc_id").equalTo(col("la.account_id")))
//                .join(office.alias("o"), col("o.id").equalTo(col("a.office_id")).and(col("o.is_deleted").equalTo(false)))
//                .join(assetClassificationSlabs.alias("acs1"), col("acs1.id").equalTo(col("la.asset_classification_slabs_id")).and(col("acs1.is_deleted").equalTo(false)))
//                .join(assetCriteriaSlabs.alias("acs2"), col("acs2.id").equalTo(col("la.asset_criteria_slabs_id")).and(col("acs2.is_deleted").equalTo(false)))
//                .join(loanApp.alias("lapp"), col("lapp.loan_account_number").equalTo(col("a.account_number")))
//                //.join(lifeInsuranceDetails.alias("lid"), col("la.account_id").equalTo(col("lid.loan_account_id")), "left_outer")
//                .join(employee.alias("e1"), col("e1.id").equalTo(col("lapp.employee_id")).and(col("e1.is_deleted").equalTo(false)))
//                .join(employee.alias("e2"), col("e2.id").equalTo(col("e1.parent_id")).and(col("e1.is_deleted").equalTo(false)))
//                .join(actorContactDetailMapping.alias("acdm"), col("acdm.actor_id").equalTo(col("e2.actor_id"))
//                        .and(col("acdm.is_deleted").equalTo(false)))
//                .join(contactDetail.alias("cd"), col("cd.id").equalTo(col("acdm.contact_detail_id")).and(col("cd.is_deleted").equalTo(false)))
//                .join(loanRestructuringDetails.alias("lrd"), col("lrd.loan_account_id").equalTo(col("a.id")), "left_outer")
//                 .join(loanAccountRestructuringDetails.alias("lard"), col("lard.id").equalTo(col("lrd.id")), "left_outer")
//                .join(glcodes.alias("g"), col("g.product_id").equalTo(col("p.id")), "left_outer")
//                .join(cuMemberDetails.alias("cmd"), col("lapp.id").equalTo(col("cmd.loan_app_id")), "left_outer")
//                .join(betDetails.alias("bet"), col("lapp.id").equalTo(col("bet.loan_app_id")), "left_outer")
//                .join(customer.alias("cust"), col("la.customer_id").equalTo(col("cust.id")).and(col("cust.is_deleted").equalTo(false)), "left_outer")
//                .selectExpr(
//                        "la.id as ACCOUNT_REFERENCE_NO",
//                        "la.account_id as LOAN_ACCOUNT_NO",
//                        "a.id as ACCOUNT_ID",
//                        "la.account_type as ACCOUNT_TYPE",
//                        "p.name as LOAN_PRODUCT",
//                        "p.product_code as PRODUCT_CODE",
//                        "lp.product_type as PRODUCT_TYPE",
//                        "aid.effective_interest_rate as EFFECTIVE_INTEREST_RATE",
//                        "lrmd.repayment_mode as REPAYMENT_MODE",
//                        "rad.repayment_account_number as REPAYMENT_ACCOUNT_NUMBER",
//                        "rmd.mandate_amount as MANDATE_AMOUNT",
//                        "ladf.loan_balance as OUTSTANDING_PRINCIPAL",
//                        "o.office_name as BRANCH_NAME",
//                        "acs1.asset_classification_slabs as ASSET_CLASSIFICATION",
//                        "acs2.asset_criteria_slabs as ASSET_CRITERIA",
//                        "lapp.loan_app_number as LOAN_APPLICATION_NO",
//                        "e.name as FIELD_OFFICER",
//                        "cd.contact_number as CUSTOMER_CONTACT_NUMBER",
//                        //"lid.loan_account_id as LIFE_INSURANCE",
//                        "lrd.restructuring_date as RESTRUCTURING_DATE",
//                        "g.code as GENERAL_LEDGER_CODE",
//                        "g.description as DESCRIPTION",
//                        "cmd.updated_by as CU_MEMBER_DETAILS",
//                        "bet.updated_by as BET_DETAILS",
//                        "cust.customer_number as CUSTOMER_NUMBER"
//                );
//    }
//
//    @Override
//    public void saveData(Dataset<Row> data) {
//        data.write()
//                .mode(SaveMode.Append)
//                .option("createTableOptions", "ENGINE = MergeTree() ORDER BY id")
//                .jdbc(databaseConfig.getClickhouseUrl(), "rbi_adf_account_details", databaseConfig.getClickhouseProperties());
//    }
//
//    @Override
//    public Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> productScheme, Dataset<Row> accountInterestDetails, Dataset<Row> assetCriteriaSlabs, Dataset<Row> assetClassificationSlabs, Dataset<Row> office, Dataset<Row> customer, Dataset<Row> loanApp, Dataset<Row> loanAppPslData, Dataset<Row> loanAccountDerivedFields) {
//        return null;
//    }
//}
