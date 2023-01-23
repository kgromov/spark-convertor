package com.spark.convetor.messaging;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SyncEvent implements Serializable {
    private static final long serialVersionUID = -2338626292552177485L;

    private UUID id;
    private DbSourceType inputType;
    private DbSourceType outputType = DbSourceType.MONGODB;
    private LocalDate startDate;
    private LocalDate endDate;
}
