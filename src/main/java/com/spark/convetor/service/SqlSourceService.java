package com.spark.convetor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.convetor.config.SparkJdbcSettings;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

@Service
@Slf4j
@RequiredArgsConstructor
public class SqlSourceService implements SourceService<DailyTemperatureDto> {
    private final DataFrameReader jdbcReader;
    private final SparkJdbcSettings jdbcSettings;
    private final ObjectMapper objectMapper;
    private final DailyTemperatureDtoRowMapper rowMapper;

    @Override
    public List<DailyTemperatureDto> readFromDB() {
        // TODO: try with more specific one - jdbc(url,table,properties)
//        return serializeAllFromJson();
//        return serializeWithMapAndBeanEncoder();
        return serializeFromRowDataset();
    }

    // Does not work due to:
    // Caused by: java.io.IOException: Cannot run program "\bin\winutils.exe": CreateProcess error=216, This version of %1 is not compatible with the version of Windows you're running.
    @SneakyThrows
    @Override
    public void exportToFileSystem() {
        Path pathToOutputCsvFile = Paths.get(".").normalize().resolve(jdbcSettings.getTableName() + ".csv");
        Dataset<Row> rows = jdbcReader.load();
      /*  Files.deleteIfExists(pathToOutputCsvFile);
        rows.write()
                .format("csv")
                .option("header", true)
                .option("delimiter", ";")
                .csv(pathToOutputCsvFile.toFile().getAbsolutePath());*/
        String header = String.join(",", rows.columns());
        String content = rows.toJSON().collectAsList().toString();

       /*
        As generic alternative for flat objects
        TypeReference<List<LinkedHashMap<String, String>>> typeRef = new TypeReference<>() {};
        List<LinkedHashMap<String, String>> linkedHashMaps = objectMapper.readValue(content, typeRef);
        List<String> body1 = linkedHashMaps.stream().map(row -> String.join(",", row.values())).collect(Collectors.toList());*/
        List<DailyTemperatureDto> temperatureDtos = objectMapper.readValue(content, new TypeReference<>() {
        });
        List<String> body = temperatureDtos.stream().map(this::mapDailyTemperatureDtoAsCommaSeparatedString).collect(Collectors.toList());
        body.add(0, header);
        Files.write(pathToOutputCsvFile, body, CREATE, WRITE);
    }

    private String mapDailyTemperatureDtoAsCommaSeparatedString(DailyTemperatureDto dto) {
        List<String> fieldValues = List.of(
                dto.getId().toString(),
                dto.getDate().toString(),
                dto.getAfternoonTemperature().toString(),
                dto.getAfternoonTemperature().toString(),
                dto.getMorningTemperature().toString(),
                dto.getNightTemperature().toString()
        );
        return String.join(",", fieldValues);
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
        stopWatch.start("serializeWithMapAndBeanEncoder");
        List<DailyTemperatureDto> temperatureDtos = jdbcReader.load().as(Encoders.bean(DailyTemperatureDto.class)).collectAsList();
        stopWatch.stop();
        log.info("Time to collectAsList with bean Encoder {} ms", stopWatch.getLastTaskTimeMillis());
        log.info("Read temperatures from SQL db (total records): {}", temperatureDtos.size());
        return temperatureDtos;
    }

    private List<DailyTemperatureDto> serializeFromRowDataset() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("serializeFromRowDataset");
        Dataset<Row> dataset = jdbcReader.load();
        Map<String, Integer> fieldsPositions = new HashMap<>();
        List<DailyTemperatureDto> temperatureDtos = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(dataset.toLocalIterator(), Spliterator.ORDERED),
                        false
                )
                .map(rowMapper::fromRow)
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
