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
import org.apache.spark.sql.types.DataTypes;
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
//                .withColumn("lac", eaiLac(functions.col("lac_dec")))
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
                // .withColumn("lac", eaiLac(functions.col("lac_dec")))
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


//        758 -930
        Dataset nr = (q.where((q.col("technology").like("NR%")).and(q.col("ru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", functions.lit("ocw/nrCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))                            //   # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))                 //    # ocw:telstraCellTypePicklist
                .withColumn("bsChannelBandwidthDownlink", UserDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("bsChannelBandwidthUplink", UserDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("localCellIdNci", functions.expr("conv(eci, 16, 10)"))                        //  # Convert eci from hex to decimal
                .withColumn("trackingAreaCode", UserDefinedFunctions.eaiInt.apply(functions.col("tac")))                             //   # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"), q.col("cell_name").alias("name"), functions.col("status"),
                        functions.col("bsChannelBandwidthDownlink"), functions.col("bsChannelBandwidthUplink"),
                        functions.col("cellType"), functions.col("localCellIdNci"), functions.col("trackingAreaCode"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),

//                        # Dynamic Attributes:
                        q.col("telstraCellAttributes|billingName"),
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

                        q.col("cgi").alias("|telstraNrCellAttributes|ncgi")
                )
        );

//#nr.show()
//#nr.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCell/csv', mode='overwrite', header=True, quoteAll=True)
        nr.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCell");


        Dataset site_to_rfCell_lookup = (q.where((q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", functions.lit("oci/site"))
                .withColumn("$action", functions.lit("LOOKUP"))

                .select(functions.col("$type"), functions.col("$action"),
                        q.col("base_station_name").alias("$refId"),
                        q.col("base_station_name").alias("name")
                )
                .distinct()
        );

//#site_to_rfCell_lookup.show()
        site_to_rfCell_lookup.write().mode("overwrite").json(Constants.SITE_TO_RFCELL_LOOKUP_PATH);


        Dataset site_to_rfCell = (q.where((q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("$type", UserDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$site", functions.array(functions.col("base_station_name")))
                .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                        functions.col("$site"), q.col("cell_name").alias("name")
                )
        );

//#site_to_rfCell.show()
        site_to_rfCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site_to_rfCell");


        Broadcast ranNumberingMap = session.sparkContext().broadcast(QuollMapConstants.ranNumberingDict, QuollUtils.classTag(Map.class));


        Dataset n = (t.where(t.col("network").isin(Arrays.asList("4G (LRAN)")).and(t.col("rbs_id").isNotNull()))
                .withColumn("name", UserDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("rbs_id"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("rbs_id").alias("id"), t.col("virtual_rnc"), //  # t.site_name,
                        t.col("status")
//                        #t.address_id,
//                        #t.nodeb_allocation_id.alias("tempestId")#    , t.du_number
                )
//        # .where("myDu is null")
//        # .where(t.node_code == "AADP')
//        # .show(200, truncate = False)
//        # .printSchema()
        );


        n = n.union(t.where(t.col("network").isin(Arrays.asList("5G (NGRAN)")).and(t.col("gnb_id").isNotNull()))
                        .withColumn("name", UserDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("gnb_id"), functions.col("node_code")))
                        .withColumn("type", functions.lit("ocw/gnbdu"))
                        .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                                t.col("gnb_id").alias("id"), t.col("virtual_rnc"),  // # t.site_name,
                                t.col("status")
//                        #t.address_id,
//                        #t.nodeb_allocation_id.alias("tempestId")#    , t.du_number
                        )
        );


        n = n.union(t.where(t.col("network").isin(Arrays.asList("3G (WRAN)")).and(t.col("rbs_id").isNotNull()))
                        .withColumn("name", UserDefinedFunctions.eaiNodeBName.apply(functions.col("site_name"), functions.col("node_code")))
                        .withColumn("type", functions.lit("ocw/nodeB"))
                        .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                                t.col("rbs_id").alias("id"), t.col("virtual_rnc"),// #  t.site_name,
                                t.col("status")
//                        #t.address_id,
//                        #t.nodeb_allocation_id.alias('tempestId')#    , t.du_number
                        )
        );


        Dataset b2 = (b
                .withColumn("type", UserDefinedFunctions.eaiBbhType.apply(functions.col("technology")))
                .withColumn("status", functions.lit("Unknown"))
                .select(b.col("name"), functions.col("type"), b.col("node_code"), b.col("id"), b.col("virtual_rnc"), functions.col("status"))
        );

        Dataset bs = (b2
                .join(n, (b2.col("name").equalTo(n.col("name"))), "inner")
                .select(b2.col("name"), b2.col("type"), b2.col("node_code"), b2.col("id"), b2.col("virtual_rnc"),
                        b2.col("status"))   //   # select the bbh side
                .union(b2.join(n, b2.col("name").equalTo(n.col("name")), "left_anti"))
                .union(n.join(b2, n.col("name").equalTo(b2.col("name")), "left_anti"))     //  # psudo right_anti
                .where(functions.col("name").isNotNull())
                .distinct());
//# n.count = 42749
//# b.count = 5698
//#n.count()
//#print(bs.count())       # 44425
//#bs.select('type').distinct().show()
//#bs.orderBy(bs.name).coalesce(1).write.csv(path='s3://emrdisco/eai_objects/baseStation/csv', mode='overwrite', header=True, quoteAll=True)
//#bs.write.json(path='s3://emrdisco/eai_objects/baseStation', mode='overwrite')
        Dataset nb_e = session.read().option("header", "true")
                .schema(QuollSchemas.enmNodeBSchema)
                .csv(Constants.enm_nodeB_PATH);


        nb_e = (nb_e
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("id", UserDefinedFunctions.eaiIdFromMecontext.apply(functions.col("mecontext")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );

        Dataset enb_e = session.read().schema(QuollSchemas.enmBaseStationSchema)
                .option("header", "true").csv(Constants.enm_nodeBS_PATH);

        enb_e = (enb_e
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
        Dataset gnbd_e = session.read()
                .option("header", "true")
                .schema(QuollSchemas.enmBaseStationSchema)
                .csv(Constants.GNODEB_DU);

        gnbd_e = (gnbd_e
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .select(functions.col("name"), functions.col("id"), gnbd_e.col("mecontext"))
                .where(functions.col("id").isNotNull()));
        Dataset gnb_e = gnbd_e;
        gnb_e = gnb_e.distinct();


        gnb_e = (gnb_e
                .withColumn("type", UserDefinedFunctions.eaiEnmGnbType.apply(functions.col("mecontext")))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
        );

        Dataset enm = nb_e;
        enm = enm.union(enb_e);
        enm = enm.union(gnb_e);
//        #enm = enm.union(bts.select(bts.name, bts.btsId.alias('id'), bts.type, bts.status, bts.name.alias('nodeCode')))
        Dataset b3 = bs.select(bs.col("name").alias("bsname"), bs.col("id"), bs.col("type").alias("bstype"),
                bs.col("status").alias("bsstatus"), bs.col("node_code").alias("bsnodeCode"));

//       # For all of the id's that match ENM and SB, keep the ENM version
        Dataset tmp1 = enm.join(b3, functions.col("id"), "left_outer").select("id", "name", "type", "status", "nodeCode");   //  #for this join get the ENM side

//# get the remaining records that are in BS but not in ENM
        Dataset tmp3 = bs.join(enm, functions.col("id"), "left_anti").select(functions.col("id"), functions.col("name"), functions.col("type"), functions.col("status"), bs.col("node_code").alias("nodeCode"));

        Dataset mbs = tmp1.union(tmp3);

//#mbs.orderBy(mbs.name).show()

//# convert all the statuses from Tempest, BBH XLSX and ENM into valid ENM statuses values
//# TPD-1275 and TPD-1328
        mbs = (mbs
                .select(mbs.col("id"), mbs.col("name"), mbs.col("type"), mbs.col("status").alias("tmp"), mbs.col("nodeCode"))
//                .withColumn("status", eaiStatus(functions.col("tmp")))
                .withColumn("$refId", functions.col("name"))
                .withColumn("$type", functions.col("type"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id"), mbs.col("name"), mbs.col("type"), functions.col("status")
                        , mbs.col("nodeCode"))
        );


//# bs gives a consolidated list of base stations from TEMPEST and BBH XLSX
//#   we now need to split these up again so that we can add in type specific fields
//#   and also tweak the fields we display

        Dataset nodeB = mbs.where(mbs.col("type").equalTo("ocw/nodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("nodeBId"), mbs.col("name"), mbs.col("status"));
        Dataset eNodeB = mbs.where(mbs.col("type").equalTo("ocw/eNodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("eNodeBId"), mbs.col("name"), mbs.col("status"));
        Dataset gNBDU = mbs.where(mbs.col("type").equalTo("ocw/gnbdu")).select(functions.col("$type"), functions.col("$refId"),
                functions.col("$action"), mbs.col("id").alias("gnbduId"), mbs.col("name"), mbs.col("status"));
//#gNBCUUP = mbs.where(mbs.type == 'ocw/gnbcuup').select('$type', '$refId', '$action', mbs.id.alias('gnbcuupId'), mbs.name, mbs.status)
//
//
//#print(nodeB.count())       # 14706
//#print(eNodeB.count())      # 25490
//#print(gNBDU.count())       # 1063
//#print(gNBCUUP.count())     # 3162


        nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB");
        eNodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB");
        gNBDU.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gNB-DU");
//#gNBCUUP.write.json(path=bucketUrl + bucketOutputPath + 'gNB-CU-UP', mode='overwrite')


        Dataset bts_to_gsmCell_lookup = (bts
                .select("$type", "$refId", "name")
                .withColumn("$action", functions.lit("lookup"))
                .select("$type", "$refId", "$action", "name")
                .distinct()
        );

//#bts_to_gsmCell_lookup.show()
        bts_to_gsmCell_lookup.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell_lookup");


//# backup :   (q.iub_rbsid.isNotNull()) & (q.technology.like('GSM%')) & (q.cell_status != 'Erroneous entry')
        Dataset bts_to_gsmCell = (q
                .where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .select(q.col("cell_name"), q.col("cell_name").substr(1, 4).alias("btsName"))
//    #.withColumn('btsId', eaiInt(F.col('iub_rbsid')))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$bts", functions.array(functions.col("btsName")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"),
                        q.col("cell_name").alias("name"), functions.col("$bts"))
        );

//#bts_to_gsmCell.show(50)
        bts_to_gsmCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell");


        Dataset nodeB_to_umtsCell_lookup = (
                nodeB.select(functions.col("$type"), nodeB.col("nodeBId").alias("$refId"), nodeB.col("name"))
                        .withColumn("$action", functions.lit("lookup"))
                        .distinct()
        );

//        nodeB_to_umtsCell_lookup.show()
        nodeB_to_umtsCell_lookup.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB_to_umtsCell_lookup");

        Dataset nodeB_to_umtsCell = (q.where((q.col("iub_rbsid").isNotNull()).and
                        (q.col("technology").like("WCDMA%")).and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("nodeBId", UserDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid"))).where(functions.col("nodeBId").isNotNull())                          //  # filter out any integer conversion errors
                .withColumn("$nodeB", functions.array(UserDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid"))))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$type", functions.lit("ocw/umtsCell"))
                .select(functions.col("$type"), functions.col("$action"),
                        q.col("cell_name").alias("$refId"), q.col("cell_name").alias("name"), functions.col("$nodeB"))
        );

        nodeB_to_umtsCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB_to_umtsCell");


        Dataset eNodeB_to_lteCell_lookup = (
                eNodeB
                        .select(functions.col("$type"),
                                eNodeB.col("eNodeBId").alias("$refId").cast(DataTypes.StringType), eNodeB.col("name"))
                        .withColumn("$action", functions.lit("lookup"))
                        .distinct()
        );

//#eNodeB_to_lteCell_lookup.show()
        eNodeB_to_lteCell_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB_to_lteCell_lookup");


        Dataset eNodeB_to_lteCell = (
                q.where((q.col("enbid_dec").isNotNull()).and(q.col("technology").like("LTE%")).
                                and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither", "rruDonor"))))
                        .withColumn("eNodeBId", UserDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")))
                        .where(functions.col("eNodeBId").isNotNull())             //# filter out any integer conversion errors
                        .withColumn("$eNodeB", functions.array(UserDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")).cast(DataTypes.StringType)))                //# Converting this BACK to string as I think the refid (above) really likes a string...FFS
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$type", functions.lit("ocw/lteCell"))
                        .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"), functions.col("$eNodeB"))
        );

//#eNodeB_to_lteCell.show()
        eNodeB_to_lteCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB_to_lteCell");


//# create a list of all of the nrCell to node relationships
        Dataset c2n = (q.where((q.col("enbid_dec").isNotNull()).and(q.col("technology").like("NR%"))
                        .and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                .withColumn("gNodeBId", UserDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")))
                .select(functions.col("gNodeBId"), q.col("cell_name"), q.col("technology"), q.col("cell_status"))
                .where(functions.col("gNodeBId").isNotNull())
        );


//# now join these with the list of base staions so that we can determine if they are du or cu

//#nrCells_to_gnbdu
        Dataset nrCells_to_gnbdu = (c2n
                .join(mbs, c2n.col("gNodeBId").equalTo(mbs.col("id")), "leftouter")
                .where(functions.col("type").equalTo("ocw/gNB-DU"))
                .select(c2n.col("gNodeBId").alias("gnbduId"),
                        functions.col("cell_name"))
//# .show(100)
//# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu/csv', mode='overwrite', header=True)
//# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbdu', mode='overwrite')
        );


//#nrCells_to_gnbcuup
        Dataset nrCells_to_gnbcuup = (c2n
                .join(mbs, c2n.col("gNodeBId").equalTo(mbs.col("id")), "leftouter")
                .where(functions.col("type").equalTo("ocw/gNB-CU-UP"))
                .select(c2n.col("gNodeBId").alias("gnbcuupId"),
                        functions.col("cell_name"))
//# .show(100)
//# .coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup/csv', mode='overwrite', header=True)
//# .write.json(path='s3://emrdisco/eai_objects/nrCells_to_gnbcuup', mode='overwrite')
        );
//# Lookup the gNodB-DU objects
        Dataset nrCells_to_gnbdu_lookup = (
                c2n
                        .join(gNBDU, (c2n.col("gNodeBId").equalTo(gNBDU.col("gnbduId"))), "inner")    //                # just get the gNodeB's that we need, alos to get the gNodeB's name
                        .withColumn("$type", functions.lit("ocw/gnbdu"))
                        .withColumn("$action", functions.lit("lookup"))
                        .select(
                                functions.col("$type"), functions.col("$action"),
                                c2n.col("gNodeBId").alias("$refId"), gNBDU.col("name")
                        )
                        .distinct()
        );

//#nrCells_to_gnbdu_lookup.show()
        nrCells_to_gnbdu_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_gnbdu_lookup");


//# update the nrCells/
        //TODO need to discuss on duplicate element
        nrCells_to_gnbdu = (
                c2n
                        .withColumn("$type", functions.lit("ocw/nrCell"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$gnbdu", functions.array(functions.col("gNodeBId")))
                        .select(
                                functions.col("$type"), functions.col("$action"),
                                c2n.col("cell_name").alias("$refId"), c2n.col("cell_name").alias("name"),
                                functions.col("$gnbdu")
                        )
                        .distinct()
        );

//#nrCells_to_gnbdu.show()
        nrCells_to_gnbdu.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_gnbdu");


        Dataset repeater = (
                q.where((q.col("rru_donor_node").equalTo("repeater")))
                        .withColumn("$type", functions.lit("ocw/repeater"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))
                        .withColumn("|telstraRepeaterAttributes|cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))
//        ocw:telstraCellTypePicklist
                        .withColumn("|telstraRepeaterAttributes|hasPriorityAssistCustomers", UserDefinedFunctions.eaiBool.apply(functions.col("priority_assist")))
                        .withColumn("|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers", UserDefinedFunctions.eaiBool.apply(functions.col("wll")))
                        .withColumn("|telstraRepeaterAttributes|isSubjecttoEmbargo", UserDefinedFunctions.eaiBool.apply(functions.col("embargo_flag")))
                        .withColumn("|telstraRepeaterAttributes|hasSpecialEvent", UserDefinedFunctions.eaiBool.apply(functions.col("special_event_cell")))
                        .withColumn("|telstraRepeaterAttributes|hasSignificantSpecialEvent", UserDefinedFunctions.eaiBool.apply(functions.col("special_event")))
                        .withColumn("|telstraRepeaterAttributes|hasHighSeasonality", UserDefinedFunctions.eaiBool.apply(functions.col("high_seasonality")))
                        .withColumn("|telstraRepeaterAttributes|mobileServiceArea", functions.col("msa"))
                        .withColumn("|telstraRepeaterAttributes|quollIndex", UserDefinedFunctions.eaiInt.apply(functions.col("cell_index")))
                        .withColumn("systemType", UserDefinedFunctions.eaiSystemType.apply(functions.col("technology")))
                        .select(
                                functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"), functions.col("status"),
                                functions.col("systemType"),
                                q.col("note").alias("comments"),
//                        # Dynamic Attributes
                                functions.col("|telstraRepeaterAttributes|cellType"),
                                q.col("cell_inservice_date").alias("|telstraRepeaterAttributes|originalOnAirDate"),
                                q.col("coverage_classification").alias("|telstraRepeaterAttributes|coverageClassification"),
                                q.col("coverage_statement").alias("|telstraRepeaterAttributes|coverageStatement"),
                                functions.col("|telstraRepeaterAttributes|hasPriorityAssistCustomers"),
                                functions.col("|telstraRepeaterAttributes|hasWirelessLocalLoopCustomers"),
                                q.col("optimisation_cluster").alias("|telstraRepeaterAttributes|optimisationCluster"),
                                q.col("owner").alias("|telstraRepeaterAttributes|wirelessServiceOwner"),
                                functions.col("|telstraRepeaterAttributes|hasSpecialEvent"),
                                functions.col("|telstraRepeaterAttributes|hasSignificantSpecialEvent"),
                                functions.col("|telstraRepeaterAttributes|isSubjecttoEmbargo"),
                                functions.col("|telstraRepeaterAttributes|hasHighSeasonality"),
                                functions.col("|telstraRepeaterAttributes|mobileServiceArea"),
                                functions.col("|telstraRepeaterAttributes|quollIndex")
                        )
        );


//#repeater.show()
        repeater.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "repeater");

//        # lookup the cells
        Dataset rfCell_to_repeater_lookup = (
                q
                        .where(q.col("rru_donor_node").equalTo("repeater").and(q.col("active_repeater_donor_node").isNotNull()))
                        .withColumn("$type", UserDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                        .withColumn("$action", functions.lit("lookup"))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("active_repeater_donor_node").alias("$refId"), q.col("active_repeater_donor_node").alias("name")
                        )
                        .distinct()
        );

//#rfCell_to_repeater_lookup.show()
        rfCell_to_repeater_lookup.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "rfCell_to_repeater_lookup");

//# update the repeaters
        Dataset rfCell_to_repeater = (
                q
                        .where((q.col("rru_donor_node").equalTo("repeater").and(q.col("active_repeater_donor_node").isNotNull())))
                        .withColumn("$type", functions.lit("ocw/repeater"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$rfCell", functions.array(functions.col("active_repeater_donor_node")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("cell_name").alias("$refId"), q.col("cell_name").alias("name"),
                                functions.col("$rfCell")
                        )
        );

//#rfCell_to_repeater.show()
        rfCell_to_repeater.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rfCell_to_repeater");


        Dataset wirelessNetwork = (q
                .where(q.col("plmn").isNotNull())
                .where(q.col("plmn").notEqual(99999))
                .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("country", functions.lit("Australia"))
                .withColumn("mcc", UserDefinedFunctions.eaiMcc.apply(functions.col("plmn")))
                .withColumn("mnc", UserDefinedFunctions.eaiMnc.apply(functions.col("plmn")))
                .withColumn("operatorName", functions.lit("Telstra"))
                .withColumn("status", functions.lit("ACTIVE"))
                .select(functions.col("$type"), functions.col("$action"),
                        q.col("plmn").alias("$refId"), q.col("plmn").alias("name"), functions.col("status"),
                        functions.col("country"), functions.col("mcc"), functions.col("mnc"), functions.col("operatorName"))
                .distinct()
                .orderBy(q.col("plmn"))
        );


//#wirelessNetwork.show()
        wirelessNetwork.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "wirelessNetwork");


        Dataset nrCells_to_wirelessNetwork = (
                q
                        .where(q.col("technology").like("NR%")
                                .and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/nrCell"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$wirelessNetworks", functions.array(functions.col("plmn")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("cell_name").alias("$refId"),
                                q.col("cell_name").alias("name"), functions.col("$wirelessNetworks"))
        );

//#nrCells_to_wirelessNetwork.show()
        nrCells_to_wirelessNetwork.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_wirelessNetwork");


        Dataset nrCells_to_wirelessNetwork_lookup = (
                q
                        .where(q.col("technology").like("NR%").and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                        .withColumn("$action", functions.lit("lookup"))
//    #.withColumn("$nrCells", functions.array(functions.col("cell_name")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("plmn").alias("$refId"),
                                q.col("plmn").alias("name")   //#,"$nrCells"
                        )
                        .distinct()
        );

//#nrCells_to_wirelessNetwork_lookup.show()
        nrCells_to_wirelessNetwork_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCells_to_wirelessNetwork_lookup");


        Dataset wirelessNetwork_to_eNodeB_lookup = (
                q
                        .where(q.col("technology").like("LTE%").and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("$type", functions.lit("ocw/wirelessNetwork"))
                        .withColumn("$action", functions.lit("lookup"))
//    #.withColumn("$nrCells",functions.array(F.col("cell_name")))
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("plmn").alias("$refId"),
                                q.col("plmn").alias("name")//#,"$nrCells"
                        )
                        .distinct()
        );

//#wirelessNetwork_to_eNodeB_lookup.show()
        wirelessNetwork_to_eNodeB_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "wirelessNetwork_to_eNodeB_lookup");


//# update eNodeB  ($wirelessNetwork)

        Dataset wirelessNetwork_to_eNodeB = (
                q
                        .where(q.col("technology").like("LTE%").and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .where(q.col("plmn").isNotNull())
                        .withColumn("eNBId", UserDefinedFunctions.eaiInt.apply(functions.col("enbid_dec")))
                        .join(eNodeB, (functions.col("eNBId").equalTo(eNodeB.col("eNodeBId"))), "inner")
                        .withColumn("$type", functions.lit("ocw/eNodeB"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$wirelessNetwork", functions.array(functions.col("plmn")))
                        .select(functions.col("$type"), functions.col("$action"),
//                        #q.enbid_dec.alias("$refId"), q.enbid_dec.alias("name"),
                                eNodeB.col("name").alias("$refId"), eNodeB.col("name"),
                                functions.col("$wirelessNetwork"))
                        .distinct()
        );

//#wirelessNetwork_to_eNodeB.show()
        wirelessNetwork_to_eNodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "wirelessNetwork_to_eNodeB");


//# lookup RNC

        Dataset rnc_to_nodeB_lookup = (
                q.where(q.col("technology").like("WCDMA%").and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .withColumn("qNBId", UserDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid")))
                        .join(nodeB, (functions.col("qNBId").equalTo(nodeB.col("nodeBId"))), "inner")
                        .withColumn("$type", functions.lit("ocw/rnc"))
                        .withColumn("$action", functions.lit("lookup"))
                        .where(q.col("bsc_rnc_node").isNotNull())
                        .select(functions.col("$type"), functions.col("$action"),
                                q.col("bsc_rnc_node").alias("$refId"), q.col("bsc_rnc_node").alias("name")
                        )
                        .distinct()
        );

//#rnc_to_nodeB_lookup.show()
        rnc_to_nodeB_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rnc_to_nodeB_lookup");


//# update nodeB  ($rnc)

        Dataset rnc_to_nodeB = (
                q.where(q.col("technology").like("WCDMA%")
                                .and(q.col("rru_donor_node").isin(Arrays.asList("remote", "neither"))))
                        .withColumn("qNBId", UserDefinedFunctions.eaiInt.apply(functions.col("iub_rbsid")))
                        .join(nodeB, (functions.col("qNBId").equalTo(nodeB.col("nodeBId"))), "inner")
                        .where(q.col("bsc_rnc_node").isNotNull())
                        .withColumn("$type", functions.lit("ocw/nodeB"))
                        .withColumn("$action", functions.lit("createOrUpdate"))
                        .withColumn("$rnc", functions.array(q.col("bsc_rnc_node")))
                        .select(functions.col("$type"), functions.col("$action"),
                                nodeB.col("name").alias("$refId"), nodeB.col("name"), functions.col("$rnc")
                        )
                        .distinct()
        );

//#rnc_to_nodeB.show()
        rnc_to_nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "rnc_to_nodeB");
    }
}
