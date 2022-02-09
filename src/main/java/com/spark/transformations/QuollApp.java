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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.IntegerType$;

import java.util.Arrays;
import java.util.Map;


public class QuollApp {
    public static void main(String[] args) {
        System.out.println("Hello ");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = ClusterConfig.getSparkSession();


        //TODO need to change to database
        Dataset qdf = session
                .read().option("header", "true").csv("/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/testone.csv");
        Transformation quollTransformations = new QuollTransformations();
        Dataset q = quollTransformations.apply(qdf);


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
        q = q.
                withColumn("|telstraCellAttributes|cellFunction", UserDefinedFunctions.eaiCellFunction.apply(q.col("cell_function"))).
                withColumn("|telstraCellAttributes|hasSpecialEvent", UserDefinedFunctions.eaiBool.apply(q.col("special_event_cell"))).
                withColumn("|telstraCellAttributes|hasSignificantSpecialEvent", UserDefinedFunctions.eaiBool.apply(q.col("special_event"))).
                withColumn("|telstraCellAttributes|hasPriorityAssistCustomers", UserDefinedFunctions.eaiBool.apply(q.col("priority_assist"))).
                withColumn("|telstraCellAttributes|hasHighSeasonality", UserDefinedFunctions.eaiBool.apply(q.col("high_seasonality"))).
                withColumn("|telstraCellAttributes|hasWirelessLocalLoopCustomers", UserDefinedFunctions.eaiBool.apply(q.col("wll"))).
                withColumn("|telstraCellAttributes|mobileSwitchingCenter", UserDefinedFunctions.eaivalidMscNode.apply(q.col("msc_node"))).
                withColumn("|telstraCellAttributes|mobileServiceArea", q.col("msa")).
                withColumn("|telstraCellAttributes|quollIndex", UserDefinedFunctions.eaiInt.apply(q.col("cell_index"))).
                withColumn("|telstraCellAttributes|closedNumberArea", UserDefinedFunctions.eaiAreaCode.apply(q.col("cna")))
                .select(q.col("*"),
                        q.col("billing_name").alias("|telstraCellAttributes|billingName"),
                        q.col("roamer").alias("|telstracellAttributes|iroaningAgreement"),
                        functions.col("|telstraCellAttributes|cellFunction"),
                        functions.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("coverage_classification").alias("|telstracellAttributes|coverageClassification"),
                        functions.regexp_replace(q.col("coverage_statement"), "[\\n\\r]+", " ").alias("|telstraCellAttributes|coverageStatement"),
                        functions.col("|telstracellAttributes|hasPriorityAssistcustomers"),
                        functions.col("|telstracellAttributes|haswirelessLocalLoopCustomers"),
                        q.col("optimisation_cluster").alias("|telstraceilAttributes|optimisationCluster"),
                        q.col("sac_dec").alias("|telstraceilAttributes|serviceAreacode").cast(IntegerType$.MODULE$),
                        q.col("owner").alias("|telstraceilAttributes|wirelessServiceOwner"),
                        functions.col("telstracellAttributes|hasSpecialEvent"),
                        functions.col("telstracellAttributes|hassignificantSpecialEvent"),
                        functions.col("telstracellattributes|mobileswitchingCentre"),
                        functions.col("telstraCellAttributes|mobileServiceArea"),
                        functions.col("telstracellAttributes|quolLindex"),
                        functions.col("telstraCellAttributes|hasHighSeasonality"));

//q.show();
        Dataset sites = (q
                .select(q.col("base_station_name").alias("name"),
                        q.col("base_station_name").alias("Srefld"),
                        q.col("state").alias("stateProv"),
                        q.col("nm_address_id").alias("siteId").cast(IntegerType$.MODULE$)//  = # cleanly converts to an integer
                )
                .distinct()
                .withColumn("$type", functions.lit("oci/site"))
                .withColumn("status", functions.lit("Live"))
                .withColumn("type", functions.lit("OTHER"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select("$type", "name", "$refId", "$action", "status", "type", "stateProv", "siteId")    //                      #this is just to re-ord
        );
//sites.show();
        sites.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site");

        Dataset bsc = (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
                .select(q.col("bsc_rnc_node").alias("name"))
                .distinct()
                .withColumn("status", functions.lit("UNKNOWN"))
                .withColumn("$type", functions.lit("ocw/bsc"))
                .withColumn("$action", functions.lit("createrOrUpdate"))
                .withColumn("$refId", functions.col("name"))
                .select("$type", "$action", "$refld", "name", "status")           //  #  datasync requires the attributes to be first
        );

        bsc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");
//                #bsc.show()

//        q.show();
        Dataset rnc = (q.where(functions.not(q.col("technology").like("GSM%")).and(q.col("bsc_rnc_node").isNotNull())))
                .select(q.col("bsc_rnc_node").alias("name"))
                .distinct()
                .withColumn("status", functions.lit("UNKNOWN"))
                .withColumn("$type", functions.lit("ocw/rnc"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$refId", functions.col("name"))
                .select("$type", "$action", "$refId", "name", "status");
        rnc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");
        rnc.show();


        Dataset bts = (q
                .where(q.col("technology").like("GSM%")) // q.iub_rbsid.isNotNull() & & (q.cell_status != "Erroneous entry")
                .select(q.col("cell_name").substr(1, 4).alias("name"), q.col("iub_rbsid").alias("btsId"))
                .withColumn("$type", functions.lit("ocw/bts"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$refunctionsId", functions.col("name"))
                .withColumn("status", functions.lit("UNKNOWN"))
                .distinct()
                .select("$type", "$action", "$refId", "name", "status", "btsId"));  // dataSync requires the $ attributes to be first
        //bts.show()
        bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts");

        Dataset bsc_to_bts_lookup = (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
                .withColumn("$type", functions.lit("ocw/bsc"))
                .withColumn("$action", functions.lit("lookup"))
                .select(functions.col("$type"),
                        q.col("bsc_rnc_node").alias("$refId"),
                        functions.col("$action"),
                        q.col("bsc_rnc_node").alias("name"))
                .distinct());
        //bsc_to_bts_lookup.show()
        bsc_to_bts_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts_lookup");


//           # now get the list of the bts's that will link to them

        Dataset bsc_to_bts = (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))// #q.iub_rbsid.isNotNull() & & (q.cell_status != 'Erroneous entry')
                .withColumn("$type", functions.lit("ocw/bts"))
                .withColumn("$bsc", functions.array(functions.col("bsc_rnc_node")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(q.col("cell_name").substr(1, 4).alias("$refId"),
                        q.col("type"), functions.col("$action"), functions.col("$bsc"),
                        q.col("cell_name").substr(1, 4).alias("name"))
                .distinct());
//            #bsc_to_bts.show()
        bsc_to_bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts");

        Dataset lte = (q.where((q.col("technology").like("LTE%")).and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", functions.lit("ocw/lteCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status"))) // ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //ocw:telstraCellTypePicklist
                .withColumn("|telstraLteCellAttributes|trackingAreaCode", UserDefinedFunctions.eaiInt.apply(functions.col("tac")))
//                # To keep here as per NNI-1336 and NNI-1622
                .withColumn("qualifiedCellId", functions.expr("conv(eci, 16, 10)"))
//                 # Convert eci from hex to decimal
                .select(functions.col("$type"),
                        q.col("cell_name").alias("$refId"),
                        functions.col("$action"),
                        q.col("cell_name").alias("name"),
                        functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 4, 99),
                                functions.lit("MHz")).alias("band'"),
                        q.col("qualifiedCellId"), functions.col("cellType"), q.col("sectorNumber"),
                        q.col("cid_dec").alias("cellId").cast(IntegerType$.MODULE$),
//
//                # cleanly converts to an integer. However need to validate as per NNI-1630
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
//                # Dynamic Attributes
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),
                        q.col("|telstraLteCellAttributes|trackingAreaCode"),
                        q.col("plmn").alias("|telstraLteCellAttributes"),
                        q.col("|plmn").cast(IntegerType$.MODULE$),
                        q.col("cgi").alias("|telstraLteCellAttributes|ecgi") // # To be changed to calculated field as per NNI-1627
                )
        );
        lte.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell");


        Dataset gsm = (q.where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))// # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //# ocw:telstraCellTypePicklist
                .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("egprsActivated", UserDefinedFunctions.eaiYN.apply(functions.col("edge")))// # values are Yes/No
                .withColumn("|telstraGsmCellAttributes|evdoEnabled", UserDefinedFunctions.eaiBool.apply(functions.col("evdo")))
                .withColumn("gprsActivated", UserDefinedFunctions.eaiYN.apply(functions.col("gprs"))) //# values are Yes/No
                .withColumn("rac", UserDefinedFunctions.eaiInt.apply(functions.col("rac_dec")))
                .withColumn("|telstraGsmCellAttributes|broadcastCode", UserDefinedFunctions.eaiInt.apply(functions.col("code_for_cell_broadcast")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"),
                        functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 4, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"), q.col("sectorNumber"), //q.col("lac_dec.alias("lac").cast(IntegerType()), # cleanly converts to an integer.
                        functions.col("lac"),
                        q.col("cgi"),
                        functions.col("egprsActivated"),
                        functions.col("gprsActivated"),
                        functions.col("rac"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"), // Dynamic Attributes
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),
                        q.col("|telstraGsmCellAttributes|broadcastCode"),
                        q.col("plmn").alias("|telstraGsmCellAttributes|plmn").cast(IntegerType$.MODULE$),
                        q.col("|telstraGsmCellAttributes|evdoEnabled"),
                        q.col("gsm03_38_coding").alias("|telstraGsmCellAttributes|gsm338Coding")));
//        gsm.show(10, 0, true);
        gsm.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gsmCell");


        Dataset umts = (q.where((q.col("technology").like("WCDMA%")).and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", functions.lit("ocw/umtsCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))                        //       # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))                     //# ocw:telstraCellTypePicklist
                .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("rac", UserDefinedFunctions.eaiRac.apply(functions.col("rac_dec")))
                .withColumn("ura", UserDefinedFunctions.eaiUra.apply(functions.col("ura")))
                .withColumn("trackingAreaCode", UserDefinedFunctions.eaiInt.apply(functions.col("tac")))                              //  # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"), functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 6, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"), q.col("cgi"), functions.col("lac"),
//                        #q.lac_dec.alias('lac').cast(IntegerType()),                       # cleanly converts to an integer.
                        functions.col("rac"),
                        functions.col("ura"),
                        q.col("cid_dec").alias("cellId").cast(IntegerType$.MODULE$),                  //  # cleanly converts to an integer.
                        q.col("cell_inservice_date").alias("originalOnAirDate"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),

//        # Dynamic Attributes:
                        q.col("|telstraCellAttributes|billingName"),
                        q.col("|telstraCellAttributes|roamingAgreement"),
                        q.col("|telstraCellAttributes|cellFunction"),
                        q.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("|telstraCellAttributes|coverageClassification"),
                        q.col("|telstraCellAttributes|coverageStatement"),
                        q.col("|telstraCellAttributes|hasPriorityAssistCustomers"),
                        q.col("|telstraCellAttributes|hasWirelessLocalLoopCustomers"),
                        q.col("|telstraCellAttributes|optimisationCluster"),
                        q.col("|telstraCellAttributes|serviceAreaCode"),
                        q.col("|telstraCellAttributes|wirelessServiceOwner"),
                        q.col("|telstraCellAttributes|hasSpecialEvent"),
                        q.col("|telstraCellAttributes|hasSignificantSpecialEvent"),
                        q.col("|telstraCellAttributes|mobileSwitchingCentre"),
                        q.col("|telstraCellAttributes|mobileServiceArea"),
                        q.col("|telstraCellAttributes|quollIndex"),
                        q.col("|telstraCellAttributes|hasHighSeasonality"),

                        q.col("ro").alias("|telstraUmtsCellAttributes|routingOrigin").cast(IntegerType$.MODULE$),               //   # cleanly converts to an integer.
                        q.col("plmn").alias("|telstraUmtsCellAttributes|plmn").cast(IntegerType$.MODULE$)
                    //  #q.hs_support.alias('|Telstra UMTS Cell Attributes|HS Supported')                      # Deprecated
                ));


            //#umts.show(vertical=True, truncate=False)
            //#umts.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/umtsCell/csv', mode='overwrite', header=True, quoteAll=True)
        umts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "umtsCell");





    }
}
