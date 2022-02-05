package com.spark.transformations;

import com.spark.transformations.config.ClusterConfig;
import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollMapConstants;
import com.spark.transformations.config.QuollSchemas;
import com.spark.transformations.service.QuollTransformations;
import com.spark.transformations.service.Transformation;
import com.spark.transformations.util.QuollUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class QuollApp {
    public static void main(String[] args) {
        System.out.println("Hello ");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = ClusterConfig.getSparkSession();


        //TODO need to change to database
        Dataset qdf=session
                .read().option("header","true").csv("/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/testone.csv");
        Transformation quollTransformations=new QuollTransformations();
        quollTransformations.apply(qdf);


        Dataset t = session.read().option("header", "true")
                .schema(QuollSchemas.nodeIdSchema)
                .csv(Constants.TEMPEST_NODE_ID_PATH);


        Dataset b = session.read().option("header", "true")
                .schema(QuollSchemas.bbhSpreadsheetSchema)
                .csv(Constants.TEMPEST_NODE_ID_PATH);


       Broadcast cellStatusMap = session.sparkContext().broadcast(QuollMapConstants.cellStatusMapDict, QuollUtils.classTag(Map.class));


    }
}
