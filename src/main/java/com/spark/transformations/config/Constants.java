package com.spark.transformations.config;

import com.spark.transformations.util.QuollUtils;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class Constants {
   public static String bucketName = "emrdisco";
//   public static String bucketUrl = "S3://emrdisco/";
   public static String bucketUrl = "/home/hduser/IdeaProjects/QuollTransformations/src/test/output/";
    public static String bucketOutputPath = "eai_objects/";


//    public static String WIFI_NNI_PATH= "s3://amrdisce/eai_objects/excluded/wifi-nni-1240";
    public static String WIFI_NNI_PATH = "/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/wifi_nni/";


//    public static String TEMPEST_NODE_ID_PATH= "S3://enrdisco/tempest/TEMPEST NODE ID.csv";
    public static String TEMPEST_NODE_ID_PATH= "/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/TEMPEST.csv";


//    public static String BBH_SPREAD_SHEETS_PATH= "s3://emrdisco/bbh/bbhSpreadsheets.csv";
    public static String BBH_SPREAD_SHEETS_PATH= "/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/bbhSpreadsheets.csv";


//    public static String SITE_TO_RFCELL_LOOKUP_PATH= "s3://emrdisco/eai_objects/site_to_rfCell_lookup";
    public static String SITE_TO_RFCELL_LOOKUP_PATH= "/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/site_to_rfCell_lookup";

//    public static String enm_nodeB_PATH="s3://emrdisco/enm/enm_nodeB.csv";
    public static String enm_nodeB_PATH="/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/enm/enm_nodeB.csv";

//    public static String enm_nodeBS_PATH="s3://emrdisco/enm/enm_eNodeB.csv";
    public static String enm_nodeBS_PATH="/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/enm/enm_nodeBs.csv";


//    public static String GNODEB_DU="s3://emrdisco/enm/enm_gNodeB-DU.csv";
    public static String GNODEB_DU="/home/hduser/IdeaProjects/QuollTransformations/src/test/resources/in/enm/enm_gNodeB-DU.csv";

    Broadcast cellStatusMap = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.cellStatusMapDict, QuollUtils.classTag(Map.class));
    Broadcast statusMapDict = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.statusMapDict, QuollUtils.classTag(Map.class));
    Broadcast cellTypeMap = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.cellTypeMapDict, QuollUtils.classTag(Map.class));
    Broadcast cellFunction = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.cellFunctionbict, QuollUtils.classTag(Map.class));
    Broadcast validscnodes = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.validiscNodeDict, QuollUtils.classTag(Map.class));
    Broadcast areaCode = ClusterConfig.getSparkSession().sparkContext().broadcast(QuollMapConstants.areaCodeDict, QuollUtils.classTag(Map.class));


}
