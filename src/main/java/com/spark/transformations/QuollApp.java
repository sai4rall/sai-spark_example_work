package com.spark.transformations;

import com.spark.transformations.config.ClusterConfig;
import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollMapConstants;
import com.spark.transformations.config.QuollSchemas;
import com.spark.transformations.service.QuollTransformations;
import com.spark.transformations.service.Transformation;
import com.spark.transformations.util.QuollUtils;
import com.spark.transformations.util.UserDefinedFunctions;
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
        Dataset q=quollTransformations.apply(qdf);


        Dataset t = session.read().option("header", "true")
                .schema(QuollSchemas.nodeIdSchema)
                .csv(Constants.TEMPEST_NODE_ID_PATH);


        Dataset b = session.read().option("header", "true")
                .schema(QuollSchemas.bbhSpreadsheetSchema)
                .csv(Constants.TEMPEST_NODE_ID_PATH);


        Broadcast cellStatusMap = session.sparkContext().broadcast(QuollMapConstants.cellStatusMapDict, QuollUtils.classTag(Map.class));
        Broadcast statusMapDict = session.sparkContext().broadcast(QuollMapConstants.statusMapDict, QuollUtils.classTag(Map.class));
        Broadcast cellTypeMap = session.sparkContext().broadcast(QuollMapConstants.cellTypeMapDict, QuollUtils.classTag(Map.class));
        Broadcast cellFunction = session.sparkContext().broadcast(QuollMapConstants.cellFunctionbict, QuollUtils.classTag(Map.class));
        Broadcast validscnodes = session.sparkContext().broadcast(QuollMapConstants.validiscNodeDict, QuollUtils.classTag(Map.class));
        Broadcast areaCode = session.sparkContext().broadcast(QuollMapConstants.areaCodeDict, QuollUtils.classTag(Map.class));
        q.
        withColumn("|telstraCellAttributes|cellFunction",UserDefinedFunctions.eaiCellFunction.apply(q.col("cell_function"))).
        withColumn("|telstraCellAttributes|hasSpecialEvent",UserDefinedFunctions.eaiBool.apply(q.col("special_event_cell"))).
        withColumn("|telstraCellAttributes|hasSignificantSpecialEvent",UserDefinedFunctions.eaiBool.apply(q.col("special_event"))).
        withColumn("|telstraCellAttributes|hasPriorityAssistCustomers",UserDefinedFunctions.eaiBool.apply(q.col("priority_assist"))).
        withColumn("|telstraCellAttributes|hasHighSeasonality",UserDefinedFunctions.eaiBool.apply(q.col("high_seasonality"))).
        withColumn("|telstraCellAttributes|hasWirelessLocalLoopCustomers",UserDefinedFunctions.eaiBool.apply(q.col("wll"))).
        withColumn("|telstraCellAttributes|mobileSwitchingCenter",UserDefinedFunctions.eaivalidMscNode.apply(q.col("msc_node"))).
        withColumn("|telstraCellAttributes|mobileServiceArea",q.col("msa")).
        withColumn("|telstraCellAttributes|quollIndex",UserDefinedFunctions.eaiInt.apply(q.col("cell_index"))).
        withColumn("|telstraCellAttributes|closedNumberArea",UserDefinedFunctions.eaiAreaCode.apply(q.col("cna")))
//                select("*",q.col(""))
        .show();

    }
}
