package com.spark.convetor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.List;

@SpringBootApplication
public class SparkConvertorApplication {

	public static void main(String[] args) {
		SpringApplication.run(SparkConvertorApplication.class, args);
	}

	@Bean
	public ApplicationRunner applicationRunner(JavaSparkContext sparkContext){
		return args -> {
			JavaRDD<String> userRDD = sparkContext.textFile("D:\\course_materials\\Hadoop\\ml-100k\\ml-100k\\u.user");
			System.out.println(userRDD.count());
			List<String> rows = userRDD.collect();
			System.out.println(rows.subList(0, 10));
			System.out.println("Partitions = "  + sparkContext.defaultMinPartitions());
		};
	}
}
