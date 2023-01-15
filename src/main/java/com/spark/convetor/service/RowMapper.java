package com.spark.convetor.service;

import org.apache.spark.sql.Row;

public interface RowMapper<T> {

    T fromRow(Row row);
}
