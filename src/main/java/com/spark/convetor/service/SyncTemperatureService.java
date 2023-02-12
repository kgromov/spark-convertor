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
import java.util.Set;

@Service
@Slf4j
@RequiredArgsConstructor
public class SyncTemperatureService {
    private final DataFrameReader jdbcReader;
    private final ConverterService converterService;

    public void syncData(SyncEvent event) {
        log.info("Start temperature sync ...");
        Dataset<Row> jdbcDataset = jdbcReader.load();
        Dataset<Row> dataset = jdbcDataset
                .select(
                        jdbcDataset.col("id"),
                        jdbcDataset.col("date"),
                        jdbcDataset.col("morningTemperature"),
                        jdbcDataset.col("afternoonTemperature"),
                        jdbcDataset.col("eveningTemperature"),
                        jdbcDataset.col("nightTemperature")
                )
                .where(jdbcDataset.col("date").$greater$eq(event.getStartDate()));
       /* List<DailyTemperatureDto> dataToSync = dataset
                .as(Encoders.bean(DailyTemperatureDto.class))
                .collectAsList();
        log.info("Data to sync: {}", dataToSync);*/
        converterService.saveToMongo(dataset, SaveMode.Append);
        log.info("Data temperature sync successfully finished");
    }
}
