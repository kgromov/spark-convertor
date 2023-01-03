package com.spark.convetor.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@RequiredArgsConstructor
public class SparkConfig {

    @PostConstruct
    public void init() {
//        System.setProperty("hadoop.home.dir", "C:\\apache\\spark\\winutils");
    }

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf()
                .setMaster("local")
//                .setSparkHome("C:/apache/spark/winutils")  // TODO: figure out
                .setAppName("Spark converter");
    }

    @Bean
    public JavaSparkContext sparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(sparkContext().sc())
                .getOrCreate();
    }
}
