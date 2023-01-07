package com.spark.convetor.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class SparkMongoDbSettings {
    private String uri;
    private String database;
    private String collection;
}
