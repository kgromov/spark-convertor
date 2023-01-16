package com.spark.convetor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spark.convetor.service.ConverterService;
import com.spark.convetor.service.NoSqlSourceService;
import com.spark.convetor.service.SqlSourceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import static com.fasterxml.jackson.databind.DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

@Slf4j
@SpringBootApplication
public class SparkConvertorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkConvertorApplication.class, args);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.configure(ADJUST_DATES_TO_CONTEXT_TIME_ZONE, false);
        return objectMapper;
    }

    @Bean
    public ApplicationRunner applicationRunner(SqlSourceService sqlSourceService,
                                               NoSqlSourceService noSqlSourceService,
                                               ConverterService converterService) {
        return args -> {
			/*List<DailyTemperatureDto> dailyTemperatureDtos = sqlSourceService.readFromDB();
			sqlSourceService.exportToFileSystem();*/
            /*List<DailyTemperatureDto> dailyTemperatureDtos = noSqlSourceService.readFromDB();
            noSqlSourceService.exportToFileSystem();*/
//            converterService.syncNoSqlWithSql();
            converterService.fromSqlToNoSql();
        };
    }
}
