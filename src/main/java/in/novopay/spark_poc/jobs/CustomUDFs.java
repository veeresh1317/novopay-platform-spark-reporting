package in.novopay.spark_poc.jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class CustomUDFs{
    public static void registerUDFs(SparkSession spark) {
        // Define 'getPymtFreq' UDF
        UDF1<String, String> getPymtFreq = (repaymentFrequency) -> {
            if (repaymentFrequency == null) {
                return "Unknown Frequency";
            }
            switch (repaymentFrequency) {
                case "WEEKLY":
                    return "Weekly";
                case "MONTHLY":
                    return "Monthly";
                case "QUARTERLY":
                    return "Quarterly";
                default:
                    return "Unknown Frequency";
            }
        };

        // Register 'getPymtFreq' UDF
        spark.udf().register("getPymtFreq", getPymtFreq, DataTypes.StringType);
    }
}
