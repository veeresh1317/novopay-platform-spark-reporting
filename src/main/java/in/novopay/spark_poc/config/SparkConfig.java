package in.novopay.spark_poc.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        return SparkSession.builder()
                .appName("YB-SPARK-CH")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions", "--add-opens java.base/sun.nio.ch=ALL-UNNAMED")
                .master("local[*]") // Change this to your cluster URL if not running locally
                .getOrCreate();
    }
}


//   spark = SparkSession.builder()
//           .appName("YB-SPARK-CH")
//           .master("local[*]")  // Replace with your Spark master URL
//           // .config("spark.ui.port", "4040")  // Optional, if you want to specify the UI port explicitly
//           .config("spark.ui.enabled", "false")
//           .config("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
//           .config("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
//           .getOrCreate();
