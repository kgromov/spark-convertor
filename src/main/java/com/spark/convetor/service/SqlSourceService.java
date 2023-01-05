package com.spark.convetor.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SqlSourceService {
    private final DataFrameReader jdbcReader;
    private final ObjectMapper objectMapper;

    public List<DailyTemperatureDto> readFromDb() {
        // TODO: try with more specific one - jdbc(url,table,properties)
        Dataset<String> jsonDS = jdbcReader.load().toJSON();
        String content = jsonDS.collectAsList().toString();
        try {
            List<DailyTemperatureDto> temperatureDtos = objectMapper.readValue(content, new TypeReference<>() {});
            log.info("Read temperatures from SQL db: {}", temperatureDtos);
            return temperatureDtos;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void exportToCsv() {

    }
}
