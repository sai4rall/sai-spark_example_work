package com.spark.transformations.service;

import org.apache.spark.sql.Dataset;

public interface Transformation {
    public Dataset apply(Dataset IDataset);
}
