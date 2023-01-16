package com.spark.convetor.service;

import java.util.List;

public interface SourceService<T> {

    List<T> readFromDB();

    void exportToFileSystem();
}
