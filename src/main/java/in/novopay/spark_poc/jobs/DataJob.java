package in.novopay.spark_poc.jobs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataJob {
    void loadData();
    Dataset<Row> processData(Dataset<Row> data);


    Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails, Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields, Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails, Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer);

    Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> accountInterestDetails,
                             Dataset<Row> loanRepaymentModeDetails, Dataset<Row> repaymentMandateDetails, Dataset<Row> repaymentAccountDetails, Dataset<Row> loanAccountDerivedFields,
                             Dataset<Row> office, Dataset<Row> assetClassificationSlabs, Dataset<Row> assetCriteriaSlabs, Dataset<Row> loanApp, Dataset<Row> lifeInsuranceDetails,
                             Dataset<Row> employee, Dataset<Row> actorContactDetailMapping, Dataset<Row> contactDetail, Dataset<Row> loanRestructuringDetails, Dataset<Row> loanAccountRestructuringDetails,
                             Dataset<Row> glcodes, Dataset<Row> cuMemberDetails, Dataset<Row> betDetails, Dataset<Row> customer);

    void saveData(Dataset<Row> data);

    Dataset<Row> processData(Dataset<Row> account, Dataset<Row> loanAccount, Dataset<Row> loanProduct, Dataset<Row> product, Dataset<Row> productScheme, Dataset<Row> accountInterestDetails, Dataset<Row> assetCriteriaSlabs, Dataset<Row> assetClassificationSlabs, Dataset<Row> office, Dataset<Row> customer, Dataset<Row> loanApp, Dataset<Row> loanAppPslData, Dataset<Row> loanAccountDerivedFields);
}
