package com.spark.convetor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.spark.convetor.messaging.DbSourceType;
import com.spark.convetor.messaging.SyncEvent;
import com.spark.convetor.service.ConverterService;
import com.spark.convetor.service.SyncTemperatureService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.time.LocalDate;

import static com.fasterxml.jackson.databind.DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE;
import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

@SpringBootApplication
@Slf4j
public class SparkConverterApplication {

    public static void main(String[] args) {
        SpringApplication.run(SparkConverterApplication.class, args);
    }

    @Bean
    public ApplicationRunner applicationRunner(SyncTemperatureService syncTemperatureService,
                                               ConverterService converterService) {
        return args -> {
//            converterService.syncNoSqlWithSql();
//            converterService.fromSqlToNoSql();

        /*    SyncEvent event = SyncEvent.builder()
                    .startDate(LocalDate.of(2023, 1, 13))
                    .endDate(LocalDate.now())
                    .inputType(DbSourceType.POSTGRES)
                    .outputType(DbSourceType.MONGODB)
                    .build();
            syncTemperatureService.syncData(event);*/
        };
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
    public StringJsonMessageConverter jsonConverter() {
        return new StringJsonMessageConverter();
    }
}
