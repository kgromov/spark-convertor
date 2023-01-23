package com.spark.convetor;

import com.spark.convetor.messaging.BrokerSettings;
import com.spark.convetor.messaging.DbSourceType;
import com.spark.convetor.messaging.SyncEvent;
import com.spark.convetor.service.ConverterService;
import com.spark.convetor.service.SyncTemperatureService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.LocalDate;

@Slf4j
@SpringBootApplication
@EnableConfigurationProperties(BrokerSettings.class)
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
}
