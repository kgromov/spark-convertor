package com.spark.convetor.service;

import com.spark.convetor.config.SparkJdbcSettings;
import com.spark.convetor.config.SparkMongoDbSettings;
import com.spark.convetor.model.DailyTemperatureDto;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Service;

import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConverterService {
    private final SparkSession sparkSession;
    private final DataFrameReader jdbcReader;
    private final SparkJdbcSettings jdbcSettings;
    private final DataFrameReader mongoReader;
    private final SparkMongoDbSettings mongoDbSettings;

    public void fromSqlToNoSql() {
        Dataset<Row> dataset = jdbcReader.load()
//                .drop("id")
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
        Dataset<Row> jdbcDataset = jdbcReader.load()/*.drop("id")*/;
        Dataset<Row> mongoDataset = mongoReader.load().drop("_id");
        JavaRDD<Row> subtract = jdbcDataset.toJavaRDD().subtract(mongoDataset.toJavaRDD());
//        sparkSession.createDataFrame(subtract, jdbcDataset.schema());


        Dataset<Row> diffDataset = /*jdbcDataset.join(mongoDataset, mongoDataset.col("date").eqNullSafe(jdbcDataset.col("date")), "left")
                .where(mongoDataset.col("date").isNull())
                .distinct()
                .withColumns(Map.of(
                        "date", mongoDataset.col("date"),
                        "morningTemperature", mongoDataset.col("morningTemperature"),
                        "afternoonTemperature", mongoDataset.col("afternoonTemperature"),
                        "eveningTemperature", mongoDataset.col("eveningTemperature"),
                        "nightTemperature", mongoDataset.col("nightTemperature")
                ));*/
                jdbcDataset.as("j").join(mongoDataset.as("m"), mongoDataset.col("date").eqNullSafe(jdbcDataset.col("date")), "left")
                        .where(mongoDataset.col("date").isNull())
                        .select("j.id",
                                "j.date",
                                "j.morningTemperature",
                                "j.afternoonTemperature",
                                "j.eveningTemperature",
                                "j.nightTemperature"
                        );
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
//        log.info("Diff = {}", dataset.toJSON().collectAsList().toString());
        log.info("Diff: count = {}", dataset.count());
        log.info("Columns: {}", dataset.columns());
        dataset.printSchema();    // just String java type converted to Date and that's it - why on earth?

        // attempt to explicitly define schema (date as string) failed
     /*   JavaRDD<Row> subtract = dataset.drop("id").toJavaRDD();
        StructType schema = new StructType()
                .add("date", "string", false)
                .add("morningTemperature", "double", true)
                .add("afternoonTemperature", "double", true)
                .add("eveningTemperature", "double", true)
                .add("nightTemperature", "double", true);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(subtract, schema);*/

        // ~7 sec for 5000 rows
//        Dataset<Row> dataFrame = correlateDate(dataset);
        // ~3.5 sec per 5000 rows
        Dataset<Row> dataFrame = correlateDateWithSqlContext(dataset);

        dataFrame
//                .as(Encoders.bean(DailyTemperatureDto.class))
                .drop("id")
                .write()
                .format("mongodb")
                .option("uri", mongoDbSettings.getUri())
                .option("connection.uri", mongoDbSettings.getUri())
                .option("database", mongoDbSettings.getDatabase())
                .option("collection", mongoDbSettings.getCollection())
                .mode(SaveMode.Overwrite)
                .save();
    }

    private <T> Dataset<Row> correlateDate(Dataset<T> dataset) {
        List<DailyTemperatureDto> dtos = dataset.as(Encoders.bean(DailyTemperatureDto.class))
                .collectAsList()
                .stream()
                .peek(dto -> dto.setDate(dto.getDate().atTime(LocalTime.MIN).toLocalDate().plusDays(1L)))
                .collect(Collectors.toList());
        return sparkSession.createDataFrame(dtos, DailyTemperatureDto.class);
    }

    private <T> Dataset<Row> correlateDateWithSqlContext(Dataset<T> dataset) {
        dataset.createOrReplaceTempView("daily_temperature");
        return dataset.sqlContext()
//                .sql("UPDATE daily_temperature SET `date` = DATE_ADD(`date`, INTERVAL 1 DAY) ");
//                .sql("UPDATE daily_temperature SET `date` = DATE_ADD(to_date(date,'yyyy-MM-dd'), 1)"); // not supported o_O
                .sql(
                        "SELECT id, morningTemperature, afternoonTemperature, eveningTemperature, nightTemperature," +
                                "DATE_ADD(to_date(date,'yyyy-MM-dd'), 1) as date " +
                                "FROM daily_temperature"
                );
    }
}
