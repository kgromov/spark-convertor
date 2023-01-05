package com.spark.convetor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.convetor.config.SparkJdbcSettings;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
@Slf4j
@RequiredArgsConstructor
public class SqlSourceService implements Serializable {
    private final SparkSession sparkSession;
    private final DataFrameReader jdbcReader;
    private final SparkJdbcSettings jdbcSettings;
    private final ObjectMapper objectMapper;

    public List<DailyTemperatureDto> readFromDb() {
        // TODO: try with more specific one - jdbc(url,table,properties)
//        return serializeAllFromJson();
//        return serializeWithMapAndBeanEncoder();
        return serializeFromRowRow();
    }

    // Does not work due to:
    // Caused by: java.io.IOException: Cannot run program "\bin\winutils.exe": CreateProcess error=216, This version of %1 is not compatible with the version of Windows you're running.
    @SneakyThrows
    public void exportToCsv() {
        Dataset<Row> rows = jdbcReader.load();
        String pathToOutputCsvFile = Paths.get(".").normalize().resolve(jdbcSettings.getTableName() + ".csv").toFile().getAbsolutePath();
        Files.deleteIfExists(Paths.get(pathToOutputCsvFile));
        rows.write()
                .format("csv")
                .option("header", true)
                .option("delimiter", ";")
                .csv(pathToOutputCsvFile);
    }

    // Around 4000 + 4000 ms
    private List<DailyTemperatureDto> serializeAllFromJson() {
        StopWatch stopWatch = new StopWatch();
        Dataset<String> jsonDS = jdbcReader.load().toJSON();
        stopWatch.stop();
        log.info("Time to load and convert to json {} ms", stopWatch.getLastTaskTimeMillis());
        stopWatch.start("collectAsList");
        String content = jsonDS.collectAsList().toString();
        stopWatch.stop();
        log.info("Time to collectAsList {} ms", stopWatch.getLastTaskTimeMillis());
        try {
            List<DailyTemperatureDto> temperatureDtos = objectMapper.readValue(content, new TypeReference<>() {
            });
            log.info("Read temperatures from SQL db (total records): {}", temperatureDtos.size());
            return temperatureDtos;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    // Does not work due to: Caused by: java.io.NotSerializableException: org.apache.spark.sql.DataFrameReader
    private List<DailyTemperatureDto> serializeWithMapAndBeanEncoder() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Load + toJson");
        Dataset<String> jsonDS = jdbcReader.load().toJSON();
        stopWatch.stop();
        log.info("Time to load and convert to json {} ms", stopWatch.getLastTaskTimeMillis());
        MapFunction<String, DailyTemperatureDto> jsonToDto = this::fromJson;
        List<DailyTemperatureDto> temperatureDtos = jsonDS.map(jsonToDto, Encoders.javaSerialization(DailyTemperatureDto.class)).collectAsList();
        log.info("Time to collectAsList {} ms", stopWatch.getLastTaskTimeMillis());
        log.info("Read temperatures from SQL db (total records): {}", temperatureDtos.size());
        return temperatureDtos;
    }

    private List<DailyTemperatureDto> serializeFromRowRow() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("serializeFromRowRow");
        Dataset<Row> dataset = jdbcReader.load();
        Map<String, Integer> fieldsPositions = new HashMap<>();
        List<DailyTemperatureDto> temperatureDtos = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(dataset.toLocalIterator(), Spliterator.ORDERED),
                        false
                )
                .map(row -> DailyTemperatureDto.builder()
                        .id(row.getLong(fieldsPositions.computeIfAbsent("id", index -> row.fieldIndex("id"))))
                        .date(row.getDate(fieldsPositions.computeIfAbsent("date", index -> row.fieldIndex("date"))).toLocalDate())
                        .morningTemperature(row.getDouble(fieldsPositions.computeIfAbsent("morningTemperature",
                                index -> row.fieldIndex("morningTemperature"))))
                        .afternoonTemperature(row.getDouble(fieldsPositions.computeIfAbsent("afternoonTemperature",
                                index -> row.fieldIndex("afternoonTemperature"))))
                        .eveningTemperature(row.getDouble(fieldsPositions.computeIfAbsent("eveningTemperature",
                                index -> row.fieldIndex("eveningTemperature"))))
                        .nightTemperature(row.getDouble(fieldsPositions.computeIfAbsent("nightTemperature",
                                index -> row.fieldIndex("nightTemperature"))))
                        .build())
                .collect(Collectors.toList());
        stopWatch.stop();
        log.info("Time to serializeFromRowRow {} ms", stopWatch.getLastTaskTimeMillis());
        log.info("Read temperatures from SQL db (total records): {}", temperatureDtos.size());
        return temperatureDtos;
    }

    private DailyTemperatureDto fromJson(String json) {
        try {
            return objectMapper.readValue(json, DailyTemperatureDto.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
