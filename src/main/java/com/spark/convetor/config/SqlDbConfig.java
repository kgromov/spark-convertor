package com.spark.convetor.config;

import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class SqlDbConfig {
    private final SparkSession sparkSession;

    //TODO: move to ConfigurationProperties
    @Value("${spark.jdbc.source.table-name}")
    private String tableName;

   /* @Bean("mySqlProperties")
    @ConfigurationProperties(prefix = "spring.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean("dataSource")
    public DataSource dataSource(@Qualifier("mySqlProperties") DataSourceProperties properties) {
        return properties
                .initializeDataSourceBuilder()
                .type(MysqlDataSource.class)
//                .type(HikariDataSource.class)
                .build();
    }*/

    @Bean("jdbcReader")
    public DataFrameReader dataFrameReader(/*@Qualifier("mySqlProperties") DataSourceProperties properties*/) {
          return sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/weather_archive")
                .option("user", "root")
                .option("password", "admin")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", tableName);
      /*  return sparkSession.read()
                .format("jdbc")
                .option("url", properties.getUrl())
                .option("user", properties.getUsername())
                .option("password", properties.getPassword())
                .option("driver", properties.getDriverClassName())
                .option("dbtable", tableName);*/
    }
}
