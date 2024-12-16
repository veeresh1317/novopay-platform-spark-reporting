package in.novopay.spark_poc.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

public interface DataJob {
    // General method for loading data
    void loadData();

    // General method for saving data
    void saveData(Dataset<Row> data);

    // Generalized processData method to accept custom logic
    Dataset<Row> processData(Function<Dataset<Row>[], Dataset<Row>> processingLogic, Dataset<Row>... datasets);
}
