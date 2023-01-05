package com.spark.convetor.config;

import lombok.*;

@Data
@NoArgsConstructor
public class SparkJdbcSettings {
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String tableName;
}
