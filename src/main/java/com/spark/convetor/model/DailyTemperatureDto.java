package com.spark.convetor.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailyTemperatureDto implements Serializable {
    private Long id;
    private LocalDate date;
    private Double morningTemperature;
    private Double afternoonTemperature;
    private Double eveningTemperature;
    private Double nightTemperature;
}
