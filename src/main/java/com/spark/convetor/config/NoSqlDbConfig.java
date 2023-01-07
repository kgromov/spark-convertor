package com.spark.convetor.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class NoSqlDbConfig {
    private final SparkSession sparkSession;

    @Bean
    @ConfigurationProperties(prefix = "spark.mongodb.source")
    public SparkMongoDbSettings mongoDbSettings() {
        return new SparkMongoDbSettings();
    }

    @Bean("mongoReader")
    public DataFrameReader mongoReader(SparkMongoDbSettings mongoDbSettings) {
        return sparkSession.read()
                .format("mongodb")
                .option("uri", mongoDbSettings.getUri())
                .option("connection.uri", mongoDbSettings.getUri())
                .option("database", mongoDbSettings.getDatabase())
                .option("collection", mongoDbSettings.getCollection());
    }
}
