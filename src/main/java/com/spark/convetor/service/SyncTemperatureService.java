package com.spark.convetor.service;

import com.spark.convetor.messaging.SyncEvent;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureService {
    private final DataFrameReader jdbcReader;
    private final ConverterService converterService;

    // TODO: decouple logic for input data sources - inject list of possible here
    public void syncData(SyncEvent event) {
        log.info("Start temperature sync ...");
        Dataset<Row> jdbcDataset = jdbcReader.load();
        Dataset<Row> dataset = jdbcDataset
                .where(jdbcDataset.col("date").$greater$eq(event.getStartDate()))
                .select(
                        jdbcDataset.col("id").as("id"),
                        jdbcDataset.col("date").as("date"),
                        jdbcDataset.col("morning_temperature").as("morningTemperature"),
                        jdbcDataset.col("afternoon_temperature").as("afternoonTemperature"),
                        jdbcDataset.col("evening_temperature").as("eveningTemperature"),
                        jdbcDataset.col("night_temperature").as("nightTemperature")
                );
       /* List<DailyTemperatureDto> dataToSync = dataset
                .as(Encoders.bean(DailyTemperatureDto.class))
                .collectAsList();
        log.info("Data to sync: {}", dataToSync);*/
        converterService.saveToMongo(dataset, SaveMode.Append);
        log.info("Data temperature sync successfully finished");
    }
}
