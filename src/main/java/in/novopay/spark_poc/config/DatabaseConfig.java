package in.novopay.spark_poc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class DatabaseConfig {

    @Value("${novopay.platform.master.datasource.protocol}")
    private String protocol;

    @Value("${novopay.platform.master.datasource.subprotocol}")
    private String subprotocol;

    @Value("${novopay.platform.master.datasource.host}")
    private String host;

    @Value("${novopay.platform.master.datasource.port}")
    private String port;

    @Value("${novopay.platform.master.datasource.db}")
    private String db;
//    @Value("${spring.datasource.url}")
//    private String springUrl;

    @Value("${novopay.platform.master.datasource.username}")
    private String ybUserName;

    @Value("${novopay.platform.master.datasource.password}")
    private String ybPassWord;

    @Value("${clickhouse.url}")
    private String clickhouseUrl;

    @Value("${clickhouse.username}")
    private String clickhouseUsername;

    @Value("${clickhouse.password}")
    private String clickhousePassword;

    @Value("${kafka.bootstrap.servers}")
    private String kafkaServers;


    public String getYugabyteUrl() {
        return String.format("%s:%s://%s:%s/%s", protocol, subprotocol, host, port, db);
    }
//    public String getYugabyteUrl() {
//        return "jdbc://" + this.host + ':' + this.port + '/' + this.db + "?autoReconnect=true";
//    }



    public Properties getYugabyteProperties() {
        Properties props = new Properties();
        props.put("user", ybUserName);
        props.put("password", ybPassWord);
        props.put("driver", "org.postgresql.Driver");
        return props;
    }

    public String getClickhouseUrl() {
        return clickhouseUrl;
    }

    public Properties getClickhouseProperties() {
        Properties props = new Properties();
        props.put("user", clickhouseUsername);
        props.put("password", clickhousePassword);
        props.put("driver", "ru.yandex.clickhouse.ClickHouseDriver");
        return props;
    }


    // Other database configuration properties and methods...

    public String getKafkaServers() {
        return kafkaServers;
    }

    // Example: If needed for other Kafka properties
    public Properties getKafkaProperties() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", kafkaServers);
        // Add additional properties if needed
        return kafkaProps;
    }
}
