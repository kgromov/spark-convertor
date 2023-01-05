package com.spark.convetor.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SqlDbConfig {
    private final SparkSession sparkSession;

    @Bean
    @ConfigurationProperties(prefix = "spark.jdbc.source")
    public SparkJdbcSettings jdbcSettings() {
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
