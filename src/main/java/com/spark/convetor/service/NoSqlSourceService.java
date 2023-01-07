package com.spark.convetor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.convetor.config.SparkMongoDbSettings;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

@Service
@Slf4j
@RequiredArgsConstructor
public class NoSqlSourceService {
    private final DataFrameReader mongoReader;
    private final SparkMongoDbSettings mongoDbSettings;
    private final ObjectMapper objectMapper;

    public List<DailyTemperatureDto> readFromDb()  {
        StopWatch stopWatch = new StopWatch();
        Dataset<Row> rowDataset = mongoReader.load();
        rowDataset.printSchema();
        Dataset<String> jsonDS = rowDataset.drop("_id").toJSON();
        stopWatch.start("readFromDb");
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

    @SneakyThrows
    public void exportToJson() {
        Path pathToOutputJsonFile = Paths.get(".").normalize().resolve(mongoDbSettings.getCollection() + ".json");
        Files.deleteIfExists(pathToOutputJsonFile);
     /*   mongoReader.load()
                .write()
                .json(pathToOutputJsonFile.toFile().getAbsolutePath());*/

        Files.write(pathToOutputJsonFile, mongoReader.load().toJSON().collectAsList(), CREATE, WRITE);
    }
}
