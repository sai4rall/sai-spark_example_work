package com.spark.transformations.config;

import org.apache.spark.sql.SparkSession;

public class ClusterConfig {
    public static SparkSession getSparkSession(){
        return SparkSession.builder().appName("QuollTransformations")
//                .master("local[1]")
                .getOrCreate();

    }
}
