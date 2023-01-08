package com.spark.convetor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.convetor.config.SparkJdbcSettings;
import com.spark.convetor.config.SparkMongoDbSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;
import scala.collection.IterableOnce;
import scala.collection.Iterator;
import scala.collection.immutable.Seq;
import scala.concurrent.JavaConversions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConverterService {
    private final DataFrameReader jdbcReader;
    private final SparkJdbcSettings jdbcSettings;
    private final DataFrameReader mongoReader;
    private final SparkMongoDbSettings mongoDbSettings;

    public void fromSqlToNoSql() {
        Dataset<Row> dataset = jdbcReader.load()
                .drop("id")
                .distinct();
        saveToMongo(dataset);
    }

    public void fromNoSqlToSql() {
        Dataset<Row> dataset = mongoReader.load()
                .drop("_id")
                .distinct();
        saveToJdbc(dataset);
    }

    public void syncNoSqlWithSql() {
        Dataset<Row> jdbcDataset = jdbcReader.load().drop("id");
        Dataset<Row> mongoDataset = mongoReader.load().drop("_id");
        Dataset<Row> diffDataset = jdbcDataset.join(mongoDataset, mongoDataset.col("date").eqNullSafe(jdbcDataset.col("date")), "left")
                .where(mongoDataset.col("date").isNull())
                .distinct();
        saveToMongo(diffDataset);
    }

    public void syncSqlWithNoSql() {
        Dataset<Row> jdbcDataset = jdbcReader.load().drop("id");
        Dataset<Row> mongoDataset = mongoReader.load().drop("_id");
        Dataset<Row> diffDataset = mongoDataset.join(jdbcDataset, mongoDataset.col("date").eqNullSafe(jdbcDataset.col("date")), "left")
                .where(jdbcDataset.col("date").isNull())
                .distinct();
        saveToJdbc(diffDataset);
    }

    private <T> void saveToJdbc(Dataset<T> dataset) {
        dataset.write()
                .format("jdbc")
                .option("url", jdbcSettings.getUrl())
                .option("user", jdbcSettings.getUsername())
                .option("password", jdbcSettings.getPassword())
                .option("driver", jdbcSettings.getDriverClassName())
                .option("dbtable", jdbcSettings.getTableName())
                .mode(SaveMode.Append)
                .save();
    }

    private <T> void saveToMongo(Dataset<T> dataset) {
        dataset.write()
                .format("mongodb")
                .option("uri", mongoDbSettings.getUri())
                .option("connection.uri", mongoDbSettings.getUri())
                .option("database", mongoDbSettings.getDatabase())
                .option("collection", mongoDbSettings.getCollection())
                .mode(SaveMode.Append)
                .save();
    }
}
