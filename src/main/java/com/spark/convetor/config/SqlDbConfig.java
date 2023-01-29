package com.spark.convetor.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@RequiredArgsConstructor
public class SqlDbConfig {
    private final SparkSession sparkSession;

    @Bean
    @Profile({"!postgres", "mysql"})
    @ConfigurationProperties(prefix = "spark.jdbc.mysql.source")
    public SparkJdbcSettings mySqlJdbcSettings() {
        return new SparkJdbcSettings();
    }

    @Bean
    @Profile({"postgres"})
    @ConfigurationProperties(prefix = "spark.jdbc.postgres.source")
    public SparkJdbcSettings postgresJdbcSettings() {
        return new SparkJdbcSettings();
    }

    @Bean("jdbcReader")
    public DataFrameReader dataFrameReader(SparkJdbcSettings jdbcSettings) {
        return sparkSession.read()
                .format("jdbc")
                .option("url", jdbcSettings.getUrl())
                .option("user", jdbcSettings.getUsername())
                .option("password", jdbcSettings.getPassword())
                .option("driver", jdbcSettings.getDriverClassName())
                .option("dbtable", jdbcSettings.getTableName());
    }
}
