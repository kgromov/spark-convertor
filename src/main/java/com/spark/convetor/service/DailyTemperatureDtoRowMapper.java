package com.spark.convetor.service;

import com.spark.convetor.model.DailyTemperatureDto;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DailyTemperatureDtoRowMapper implements RowMapper<DailyTemperatureDto> {

    public static final String MORNING_TEMPERATURE = "morningTemperature";
    public static final String AFTERNOON_TEMPERATURE = "afternoonTemperature";
    public static final String EVENING_TEMPERATURE = "eveningTemperature";
    public static final String NIGHT_TEMPERATURE = "nightTemperature";

    @Override
    public DailyTemperatureDto fromRow(Row row) {
        Map<String, Integer> fieldsPositions = new HashMap<>();
        return DailyTemperatureDto.builder()
                .id(row.getLong(fieldsPositions.computeIfAbsent("id", index -> row.fieldIndex("id"))))
                .date(row.getDate(fieldsPositions.computeIfAbsent("date", index -> row.fieldIndex("date"))).toLocalDate())
                .morningTemperature(row.getDouble(fieldsPositions.computeIfAbsent(MORNING_TEMPERATURE,
                        index -> row.fieldIndex(MORNING_TEMPERATURE))))
                .afternoonTemperature(row.getDouble(fieldsPositions.computeIfAbsent(AFTERNOON_TEMPERATURE,
                        index -> row.fieldIndex(AFTERNOON_TEMPERATURE))))
                .eveningTemperature(row.getDouble(fieldsPositions.computeIfAbsent(EVENING_TEMPERATURE,
                        index -> row.fieldIndex(EVENING_TEMPERATURE))))
                .nightTemperature(row.getDouble(fieldsPositions.computeIfAbsent(NIGHT_TEMPERATURE,
                        index -> row.fieldIndex(NIGHT_TEMPERATURE))))
                .build();
    }
}
