package com.spark.transformations.util;

import com.spark.transformations.LookUpData;
import com.spark.transformations.config.ClusterConfig;
import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollSchemas;
import org.apache.hadoop.shaded.com.google.common.base.CharMatcher;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QuollUtils implements Serializable {
    static  Logger logger = Logger.getLogger(QuollUtils.class);
    UserDefinedFunctions userDefinedFunctions;
    Broadcast<LookUpData> lookUpDataBroadcast;
     public QuollUtils(){
         LookUpData lookUpData=new LookUpData();
         lookUpDataBroadcast= ClusterConfig.getSparkSession().sparkContext().broadcast(lookUpData, classTag(LookUpData.class));
         this.lookUpDataBroadcast=lookUpDataBroadcast;
         this.userDefinedFunctions=new UserDefinedFunctions(this);
     }

    public Dataset readFile(SparkSession session, StructType schema, String path) {
        if (schema == null) {
            return session
                    .read().option("header", "true").csv(path);
        } else {
            return session.read().option("header", "true")
                    .schema(schema)
                    .csv(path);
        }

    }

    public Dataset applyInitialTrabsformaions(Dataset qdf) {
//# Add in the sector
        System.out.println("qdf");
        Dataset<Row> q = qdf.select(qdf.col("*"),
                qdf.col("cell_name").substr(5, 2).alias("utchar"),
                qdf.col("cell_name").substr(7, 1).alias("sector"));


        q = q.drop("bts_instance_display_name", "clli_number_bcd", "clli_number_hex",
                "do_rnc", "multi_nids", "nid", "switch_number","zone_timer" ,"xrtt_enabled",
                "cell_fro_id", "cid_hex", "lac_hex", "enbid_hex");



        q.where(q.col("base_station_name").like("%WIFI%"))
                .write().mode("overwrite").json(Constants.WIFI_NNI_PATH);


        q = q.where(functions.not(q.col("base_station_name").like("%WIFI%")));

//        UserDefinedFunction ea1sectorNumber = functions.udf((String s) -> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);

        q = q.withColumn("sectorNumber",   userDefinedFunctions.eaisectorNumber.apply(functions.col("sector")));
        q = q.where(functions.not(q.col("cell_status").isin("Erroneous entry", "Removed")));

        return q;
    }


    public Dataset addAdditionalAttributes(Dataset q) {
        return q.withColumn("|telstraCellAttributes|cellFunction",   userDefinedFunctions.eaiCellFunction.apply(q.col("cell_function"))).
                withColumn("|telstraCellAttributes|hasSpecialEvent",  userDefinedFunctions.eaiBool.apply(q.col("special_event_cell"))).
                withColumn("|telstraCellAttributes|hasSignificantSpecialEvent",  userDefinedFunctions.eaiBool.apply(q.col("special_event"))).
                withColumn("|telstraCellAttributes|hasPriorityAssistCustomers",  userDefinedFunctions.eaiBool.apply(q.col("priority_assist"))).
                withColumn("|telstraCellAttributes|hasHighSeasonality",  userDefinedFunctions.eaiBool.apply(q.col("high_seasonality"))).
                withColumn("|telstraCellAttributes|hasWirelessLocalLoopCustomers",  userDefinedFunctions.eaiBool.apply(q.col("wll"))).
                withColumn("|telstraCellAttributes|mobileSwitchingCentre",  userDefinedFunctions.eaivalidMscNode.apply(q.col("msc_node"))).
                withColumn("|telstraCellAttributes|mobileServiceArea", q.col("msa")).
                withColumn("|telstraCellAttributes|quollIndex",  userDefinedFunctions.eaiInt.apply(q.col("cell_index"))).
                withColumn("|telstraCellAttributes|closedNumberArea",  userDefinedFunctions.eaiAreaCode.apply(q.col("cna")))
                .select(q.col("*"),
                        q.col("billing_name").alias("|telstraCellAttributes|billingName"),
                        q.col("roamer").alias("|telstracellAttributes|roamingAgreement"),
                        functions.col("|telstraCellAttributes|cellFunction"),
                        functions.col("|telstraCellAttributes|closedNumberArea"),
                        q.col("coverage_classification").alias("|telstracellAttributes|coverageClassification"),
                        functions.regexp_replace(q.col("coverage_statement"), "[\\n\\r]+", " ").alias("|telstraCellAttributes|coverageStatement"),
                        functions.col("|telstracellAttributes|hasPriorityAssistcustomers"),
                        functions.col("|telstracellAttributes|haswirelessLocalLoopCustomers"),
                        q.col("optimisation_cluster").alias("|telstracellAttributes|optimisationCluster"),
                        q.col("sac_dec").alias("|telstracellAttributes|serviceAreacode").cast(IntegerType$.MODULE$),
                        q.col("owner").alias("|telstracellAttributes|wirelessServiceOwner"),
                        functions.col("|telstraCellAttributes|hasSpecialEvent"),
                        functions.col("|telstracellAttributes|hassignificantSpecialEvent"),
                        functions.col("|telstracellattributes|mobileSwitchingCentre"),
                        functions.col("|telstraCellAttributes|mobileServiceArea"),
                        functions.col("|telstracellAttributes|quolLindex"),
                        functions.col("|telstraCellAttributes|hasHighSeasonality"));
    }


    public Dataset cleanlyConvertssitesToInteger(Dataset q) {
        return (q
                .select(q.col("base_station_name").alias("name"),
                        q.col("base_station_name").alias("$refId"),
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
    }


    public Dataset transformBsc(Dataset q) {
        return (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
                .select(q.col("bsc_rnc_node").alias("name"))
                .distinct()
                .withColumn("status", functions.lit("UNKNOWN"))
                .withColumn("$type", functions.lit("ocw/bsc"))
                .withColumn("$action", functions.lit("createrOrUpdate"))
                .withColumn("$refId", functions.col("name"))
                .select("$type", "$action", "$refId", "name", "status")           //  #  datasync requires the attributes to be first
        );
    }

    public Dataset transformRnc(Dataset q) {
        return (q.where(functions.not(q.col("technology").like("GSM%")).and(q.col("bsc_rnc_node").isNotNull()))
                .select(q.col("bsc_rnc_node").alias("name"))
                .distinct()
                .withColumn("status", functions.lit("UNKNOWN"))
                .withColumn("$type", functions.lit("ocw/rnc"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$refId", functions.col("name"))
                .select("$type", "$action", "$refId", "name", "status")
);
    }

    public Dataset transfromBts(Dataset q) {
        return  (q
                .where(q.col("technology").like("GSM%")) // q.iub_rbsid.isNotNull() & & (q.cell_status != "Erroneous entry")
                .select(q.col("cell_name").substr(1, 4).alias("name"), q.col("iub_rbsid").alias("btsId"))
                .withColumn("$type", functions.lit("ocw/bts"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$refId", functions.col("name"))
                .withColumn("status", functions.lit("UNKNOWN"))
                .distinct()
                .select("$type", "$action", "$refId", "name", "status", "btsId"));  // dataSync requires the $ attributes to be first
        //bts.show()
    }
    public Dataset transfromBscToBtsLookup(Dataset q) {
        return    (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))
                .withColumn("$type", functions.lit("ocw/bsc"))
                .withColumn("$action", functions.lit("lookup"))
                .select(functions.col("$type"),
                        q.col("bsc_rnc_node").alias("$refId"),
                        functions.col("$action"),
                        q.col("bsc_rnc_node").alias("name"))
                .distinct());
    }
    public Dataset transfromBscToBts(Dataset q) {
        return        (q.where(q.col("technology").like("GSM%").and(q.col("bsc_rnc_node").isNotNull()))// #q.iub_rbsid.isNotNull() & & (q.cell_status != 'Erroneous entry')
                .withColumn("$type", functions.lit("ocw/bts"))
                .withColumn("$bsc", functions.array(functions.col("bsc_rnc_node")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(q.col("cell_name").substr(1, 4).alias("$refId"),
                        functions.col("$type"), functions.col("$action"), functions.col("$bsc"),
                        q.col("cell_name").substr(1, 4).alias("name"))
                .distinct());
    }
    public Integer genSectorNumber(String sector) {

        if (sector != null && !sector.isBlank()) {
            if (Arrays.asList("0", "1", "2", "3", "4", "5", "6", "7", "8", "9").indexOf(sector) > -1) {
                return Integer.parseInt(sector);
            } else {
                return null;
            }
        } else {
            return null;
        }
    }


    public String mapCellStatus(String qStatus) {
//            # note: we cannot have a status on None as the record will not load into EAI
        return lookUpDataBroadcast.getValue().getCellStatusMapDict().get(qStatus) != null ? lookUpDataBroadcast.getValue().getCellStatusMapDict().get(qStatus) : lookUpDataBroadcast.getValue().getCellStatusMapDict().get("Concept");
    }

    public String mapStatus(String Status) {
//          # note: we cannot have a status on None as the record will not load into EAI
        return lookUpDataBroadcast.getValue().getStatusMapDict().get(Status) != null ? lookUpDataBroadcast.getValue().getStatusMapDict().get(Status) : lookUpDataBroadcast.getValue().getStatusMapDict().get("Unknown");
    }

    public String mapCellType(String qType) {
//          # note: we cannot have a status on None as the record will not load into EAI
        return lookUpDataBroadcast.getValue().getCellTypeMapDict().get(qType) != null ? lookUpDataBroadcast.getValue().getCellTypeMapDict().get(qType) : lookUpDataBroadcast.getValue().getCellTypeMapDict().get("TBA");
    }

    public String mapCellFunction(String qFunction) {
        if (qFunction != null && isInteger(qFunction)) {
            int temp = Integer.parseInt(qFunction);
            return lookUpDataBroadcast.getValue().getCellFunctionbict().get(temp);
        } else {
            return null;
        }
    }

    //    # use eaiInt instead
//# convert RAC to an int
    public Integer cleanRac(String qrac) {
        if (qrac != null && isInteger(qrac)) {
            return Integer.parseInt(qrac);
        } else {
            return null;
        }
    }

    public String validateMscNode(String node) {
        return node != null ? lookUpDataBroadcast.getValue().getValidiscNodeDict().get(node) : null;
    }

    //        # convert a string flag into a Boolean
    public boolean cleanBool(String qVal) {
        if (qVal != null &&
                (qVal.toUpperCase().equalsIgnoreCase("TRUE") ||
                        (qVal.toUpperCase().equalsIgnoreCase("SIGNIFICANT")))) {
            return true;
        } else {
            return false;
        }
    }

    public String cleanYN(String qVal) {
        if (qVal != null &&
                (qVal.toUpperCase().equalsIgnoreCase("TRUE"))) {
            return "YES";

        } else {
            return "NO";
        }
    }

    //    # the LAC needs to be converted to Int in the range 1->65,535. Values of are to be changed to None.
    public Integer cleanLac(String qval) {
        if (qval != null && isInteger(qval)) {
            Integer lac = Integer.parseInt(qval);
            if (lac > 0)
                return lac;
            else
                return null;
        } else {
            logger.info("Error converting LAC:" + qval);
            return null;
        }
    }

    //TODO needto check why we have two implementations for cleanRacVal
//# the RAC needs to be converted to Int in the range 0->255. Values above 255 are changed to None
    public Integer cleanRacVal(String qval) {
        if (qval != null && isInteger(qval)) {
            Integer rac = Integer.parseInt(qval);
            if (rac < 255)
                return rac;
            else
                return null;
        } else {
            logger.info("Error converting RAC::" + qval);
            return null;
        }
    }

    public Integer cleanInt(String qval) {
        if (qval != null && isInteger(qval)) {
            return Integer.parseInt(qval);
        } else {
            logger.info("Error converting" + qval + "to int");
            return null;
        }
    }

    public String mapAreaCode(String qAc) {
        if (qAc != null && isInteger(qAc)) {
            Integer ac = Integer.parseInt(qAc);
            return lookUpDataBroadcast.getValue().getAreaCodeDict().get(ac);
        } else {
            logger.info("Error converting area code" + qAc);
            return null;
        }
    }


    public Integer cleanUra(String qStr) {
        if (qStr == null) {
            return null;
        } else {
            if (isInteger(qStr.split(":")[0])) {
                return Integer.parseInt(qStr.split(":")[0]);
            } else {
                logger.warn("error cleaning URA field" + qStr);
                return null;
            }

        }
    }

    public <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    public boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public String technologyToType(String sTech) {
        if (sTech != null && !sTech.isBlank()) {
            if (sTech.indexOf("NR") > -1)
                return "On/nrCell";
            if (sTech.indexOf("LTE") > -1)
                return "ocw/lteCell";
            else if (sTech.indexOf("WCDMA") > -1)
                return "Ocw/umtsCell";
            else if (sTech.indexOf("GSM") > -1)
                return "ocw/gsmCell";
            else
                return null;

        } else {
            return null;
        }
    }

    public String getBbhType(String tech) {
        if (tech == null)
            return null;
        else {
            if (tech.indexOf("LTE") >= 0) {
                return "ocw/eNodeB";
            }
            if (tech.indexOf("NR") >= 0) {
                return "ocw/gnbdu";
            }
            return null;
        }
    }


    public Integer cleanTechnology(String qStr) {
        if (qStr == null) {
            return null;
        } else {
            try {
//                 # 'NR26G' == 'NR26000'
                String s = qStr.replace("G", "000").substring(2);
                return Integer.parseInt(s);
            } catch (Exception e) {
                logger.warn("error cleaning technology field:" + qStr);
                return null;
            }
        }
    }
    public String genEGNodeBName(String du, String site, Integer nid, String nodeCode) {
        /*
        UDF for building the DU if there is no exsting du field.
         */
        if (du == null) {
            try {
                String incChar = "";
                Pattern lte1DigitPattern = Pattern.compile("\\(LTE[1-9]\\)");
                Matcher m1d = lte1DigitPattern.matcher(site);
                Pattern lte2DigitPattern = Pattern.compile("\\(LTE[1-9][0-9]\\)");
                Matcher m2d = lte2DigitPattern.matcher(site);
// # check for 2 digit increment numbers and convert them to letters
                if (m2d.find()){
                    site = site.substring(m2d.start());
                    incChar = ""+site.replaceAll("[^0-9]", "");
                    incChar = lookUpDataBroadcast.getValue().getRanNumberingDict().get(incChar);   //# convert to a character
                }else if(m1d.find()){
                    site = site.substring(m1d.start());
                    incChar = ""+site.replaceAll("[^0-9]", "");
                }else{
//                    # search for single digit increment numbers.
//                # extract out the section within the (), assuming it is at the end of the site string
                    site = site.substring(site.indexOf('('));
//                   # replace '1-x' with just 1.  This seems to be the case for all instances that have a du_number populated.
                    site = site.replace("1-2", "1");
                    site = site.replace("1-3", "1");
                    site = site.replace("1-4", "1");
//                     # first remove '3G', '4G', '5G'
                    site = site.replace("3G", "");
                    site = site.replace("4G", "");
                    site = site.replace("5G", "");
//                     # Extract out the reamining digits
                    incChar = ""+site.replaceAll("[^0-9]", "");

                }
                if (incChar.length() == 1){
                    int id=nid / 100000;
                    return nodeCode + incChar + id;
                }
                else if(incChar.length() == 0) {
                    // if no number found assume '1'
                    int id=nid / 100000;
                    return nodeCode + "1" +id;
                }else {
                    return null;
                }

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
        return nodeCode + String.valueOf(du);
    }


    public String genNodeBName(String site, String nodeCode) {
        try {
            Pattern pattern = Pattern.compile("\\([1-9]\\)");
            Matcher m = pattern.matcher(site);
            if(m.find()){
                site = site.substring(m.start());
                String incChar = ""+site.replaceAll("[^0-9]", "");
                return nodeCode + incChar;
            }
            pattern = Pattern.compile("\\([1-9][0-9]\\)");
            m = pattern.matcher(site);
            if(m.find()){
                site = site.substring(m.start());
                String incChar = ""+site.replaceAll("[^0-9]", "");
                return nodeCode + lookUpDataBroadcast.getValue().getRanNumberingDict().get(incChar);
            }

            return nodeCode + "1";
        } catch (Exception e) {
            return null;
        }
    }


    public String extractNameFromMecontext(String qStr, Boolean paddOne) {
        if (paddOne == null) {
            paddOne = false;
        }
        if (qStr == null) {
            return null;
        } else {
            try {
                if (paddOne) {
                    String tmp = qStr.split("_")[1];
                    if (tmp.length() == 4) {
                        return tmp + "1";
                    } else {
                        return tmp;
                    }
                } else {
                    return qStr.split("_")[1];
                }

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public Integer extractIdFromMecontext(String qStr) {

        if (qStr == null) {
            return null;
        } else {
            try {
                String[] qStrArr = qStr.split("_");
                return Integer.parseInt(qStr.split("_")[qStrArr.length - 1]);
            } catch (Exception e) {
                logger.warn("error extracting ID from mecontext for:" + qStr);
                return null;
            }
        }
    }

    public String enmGnbType(String mecontext) {
        if (mecontext == null) {
            return null;
        }
        if (mecontext.indexOf("_BBH_") >= 0) {
//            #return "ocw/gnbcuup"
//            # ^ is not required as we now are not including the CU objects.
            return "ocw/gnbdu";
        } else {
            return "ocw/gnbdu";
        }
    }


    public Dataset transformLte(Dataset q) {
        return (q.where((q.col("technology").like("LTE%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/lteCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status",  userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status"))) // ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType",  userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //ocw:telstraCellTypePicklist
                .withColumn("|telstraLteCellAttributes|trackingAreaCode",  userDefinedFunctions.eaiInt.apply(functions.col("tac")))
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
                        functions.col("qualifiedCellId"),
                        functions.col("cellType"), q.col("sectorNumber"),
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
                        functions.col("|telstraLteCellAttributes|trackingAreaCode")//,
//                        q.col("plmn").alias("|telstraLteCellAttributes"),//TODO need to check next 3 lines
//                        q.col("|plmn").cast(IntegerType$.MODULE$),
//                        q.col("cgi").alias("|telstraLteCellAttributes|ecgi") // # To be changed to calculated field as per NNI-1627
                )
        );
    }
    public Dataset transformGsm(Dataset q){
        return   (q.where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status",  userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))// # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType",  userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type"))) //# ocw:telstraCellTypePicklist
//                .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("egprsActivated",  userDefinedFunctions.eaiYN.apply(functions.col("edge")))// # values are Yes/No
                .withColumn("|telstraGsmCellAttributes|evdoEnabled",  userDefinedFunctions.eaiBool.apply(functions.col("evdo")))
                .withColumn("gprsActivated",  userDefinedFunctions.eaiYN.apply(functions.col("gprs"))) //# values are Yes/No
                .withColumn("rac",  userDefinedFunctions.eaiInt.apply(functions.col("rac_dec")))
                .withColumn("|telstraGsmCellAttributes|broadcastCode",  userDefinedFunctions.eaiInt.apply(functions.col("code_for_cell_broadcast")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"),
                        functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 4, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"), q.col("sectorNumber"), //q.col("lac_dec.alias("lac").cast(IntegerType()), # cleanly converts to an integer.
//                        functions.col("lac"),//TODO fix this
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
                        functions.col("|telstraGsmCellAttributes|broadcastCode"),
                        q.col("plmn").alias("|telstraGsmCellAttributes|plmn").cast(IntegerType$.MODULE$),
                        functions.col("|telstraGsmCellAttributes|evdoEnabled"),
                        q.col("gsm03_38_coding").alias("|telstraGsmCellAttributes|gsm338Coding")));
    }

    public Dataset transformUmts(Dataset q) {
       return (q.where((q.col("technology").like("WCDMA%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/umtsCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status",  userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))                        //       # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType",  userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))                     //# ocw:telstraCellTypePicklist
//                 .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("rac",  userDefinedFunctions.eaiRac.apply(functions.col("rac_dec")))
                .withColumn("ura",  userDefinedFunctions.eaiUra.apply(functions.col("ura")))
                .withColumn("trackingAreaCode",  userDefinedFunctions.eaiInt.apply(functions.col("tac")))                              //  # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"),
                        functions.col("$action"), q.col("cell_name").alias("name"), functions.col("status"),
                        functions.concat(functions.substring(q.col("technology"), 6, 99),
                                functions.lit(" MHz")).alias("band"),
                        functions.col("cellType"),
                        q.col("cgi"),
//                        functions.col("lac"),//TODO fix this
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
    }

    public Dataset transformNr(Dataset q) {
        return (q.where((q.col("technology").like("NR%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("ocw/nrCell"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("status",  userDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))                            //   # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType",  userDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))                 //    # ocw:telstraCellTypePicklist
                .withColumn("bsChannelBandwidthDownlink",  userDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("bsChannelBandwidthUplink",  userDefinedFunctions.eaiChannel.apply(functions.col("technology")))
                .withColumn("localCellIdNci", functions.expr("conv(eci, 16, 10)"))                        //  # Convert eci from hex to decimal
                .withColumn("trackingAreaCode",  userDefinedFunctions.eaiInt.apply(functions.col("tac")))                             //   # Convert string to int via udf
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"), q.col("cell_name").alias("name"), functions.col("status"),
                        functions.col("bsChannelBandwidthDownlink"), functions.col("bsChannelBandwidthUplink"),
                        functions.col("cellType"), functions.col("localCellIdNci"), functions.col("trackingAreaCode"),
                        functions.regexp_replace(q.col("note"), "[\\n\\r]+", " ").alias("comments"),
                        q.col("cell_inservice_date").alias("originalOnAirDate"),

//                        # Dynamic Attributes:
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
                        q.col("cgi").alias("|telstraNrCellAttributes|ncgi")
                )
        );
    }

    public Dataset transformSiteToRfCellLookUp(Dataset q) {
        return (q.where((q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type", functions.lit("oci/site"))
                .withColumn("$action", functions.lit("LOOKUP"))

                .select(functions.col("$type"), functions.col("$action"),
                        q.col("base_station_name").alias("$refId"),
                        q.col("base_station_name").alias("name")
                )
                .distinct()
        );

    }

    public Dataset transformSiteToRfCell(Dataset q) {
        return (q.where((q.col("rru_donor_node").isin("remote", "neither")))
                .withColumn("$type",  userDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$site", functions.array(functions.col("base_station_name")))
                .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                        functions.col("$site"), q.col("cell_name").alias("name")
                )
        );
    }

    public Dataset transform4GLRAN(Dataset t) {
        return (t.where(t.col("network").isin("4G (LRAN)").and(t.col("rbs_id").isNotNull()))
                .withColumn("name",  userDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("rbs_id"), functions.col("node_code")))
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
    }

    public Dataset transform5GNGRAN(Dataset t) {
        return (t.where(t.col("network").isin("5G (NGRAN)").and(t.col("gnb_id").isNotNull()))
                .withColumn("name",  userDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("gnb_id"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/gnbdu"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("gnb_id").alias("id"), t.col("virtual_rnc"),  // # t.site_name,
                        t.col("status")
//                        #t.address_id,
//                        #t.nodeb_allocation_id.alias("tempestId")#    , t.du_number
                )
        );
    }

    public Dataset transform3GWRAN(Dataset t) {
        return  (t.where(t.col("network").isin("3G (WRAN)").and(t.col("rbs_id").isNotNull()))
                .withColumn("name",  userDefinedFunctions.eaiNodeBName.apply(functions.col("site_name"), functions.col("node_code")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .select(functions.col("name"), functions.col("type"), t.col("node_code"),
                        t.col("rbs_id").alias("id"), t.col("virtual_rnc"),// #  t.site_name,
                        t.col("status")
//                        #t.address_id,
//                        #t.nodeb_allocation_id.alias('tempestId')#    , t.du_number
                )
        );
    }

    public Dataset transformnbe(Dataset nb_e) {
        return (nb_e
                .withColumn("name",  userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("id",  userDefinedFunctions.eaiIdFromMecontext.apply(functions.col("mecontext")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformenbE(Dataset enb_e) {
        return (enb_e
                .withColumn("name",  userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformGnbdE(Dataset gnbd_e) {
        return  (gnbd_e
                .withColumn("name",  userDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .select(functions.col("name"), functions.col("id"), gnbd_e.col("mecontext"))
                .where(functions.col("id").isNotNull()));
    }

    public Dataset transformGnbE(Dataset gnb_e) {
       return  (gnb_e
                .withColumn("type",  userDefinedFunctions.eaiEnmGnbType.apply(functions.col("mecontext")))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
        );
    }

    public Dataset transformBtsToGsmCell(Dataset q) {
        return  (q
                .where((q.col("technology").like("GSM%")).and(q.col("rru_donor_node").isin("remote", "neither")))
                .select(q.col("cell_name"), q.col("cell_name").substr(1, 4).alias("btsName"))
//    #.withColumn('btsId', eaiInt(F.col('iub_rbsid')))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$type", functions.lit("ocw/gsmCell"))
                .withColumn("$bts", functions.array(functions.col("btsName")))
                .select(functions.col("$type"), q.col("cell_name").alias("$refId"), functions.col("$action"),
                        q.col("cell_name").alias("name"), functions.col("$bts"))
        );
    }

    public Dataset transformBtsToGsmCelLoopUP(Dataset bts) {
       return  (bts
                .select("$type", "$refId", "name")
                .withColumn("$action", functions.lit("lookup"))
                .select("$type", "$refId", "$action", "name")
                .distinct()
        );
    }

    public Dataset transformMbs(Dataset mbs) {
        return (mbs
                .select(mbs.col("id"), mbs.col("name"), mbs.col("type"), mbs.col("status").alias("tmp"), mbs.col("nodeCode"))
                .withColumn("status",  userDefinedFunctions.eaiStatus.apply(functions.col("tmp")))
                .withColumn("$refId", functions.col("name"))
                .withColumn("$type", functions.col("type"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id"), mbs.col("name"), mbs.col("type"), functions.col("status")
                        , mbs.col("nodeCode"))
        );
    }

    public Dataset transformBsToB3(Dataset bs) {
        return bs.select(bs.col("name").alias("bsname"), bs.col("id"), bs.col("type").alias("bstype"),
                bs.col("status").alias("bsstatus"), bs.col("node_code").alias("bsnodeCode"));
    }

    public Dataset transformenmToMbs(Dataset enm,Dataset b3,Dataset bs) {

//       # For all of the id's that match ENM and SB, keep the ENM version
        Dataset tmp1 = enm.join(b3, enm.col("id").equalTo(b3.col("id")), "left_outer")
                .select(b3.col("id"), enm.col("name"), enm.col("type"), enm.col("status"), enm.col("nodeCode"));   //  #for this join get the ENM side

//# get the remaining records that are in BS but not in ENM
        Dataset tmp3 = bs.join(enm, bs.col("id").equalTo(enm.col("id")), "left_anti")
                .select(bs.col("id"), bs.col("name"), bs.col("type"), bs.col("status"),
                        bs.col("node_code").alias("nodeCode"));
        return tmp1.union(tmp3);
    }

    public Dataset joinBsAndn(Dataset b2, Dataset n) {
      return   (b2
                .join(n, (b2.col("name").equalTo(n.col("name"))), "inner")
                .select(b2.col("name"), b2.col("type"), b2.col("node_code"), b2.col("id"), b2.col("virtual_rnc"),
                        b2.col("status"))   //   # select the bbh side
                .union(b2.join(n, b2.col("name").equalTo(n.col("name")), "left_anti"))
                .union(n.join(b2, n.col("name").equalTo(b2.col("name")), "left_anti"))     //  # psudo right_anti
                .where(functions.col("name").isNotNull())
                .distinct());
    }

    public Dataset generateBsDataset(Dataset t, Dataset b) {
        Dataset n = transform4GLRAN(t);
        n = n.union(transform5GNGRAN(t));
        n = n.union(transform3GWRAN(t));


        Dataset b2 = (b
                .withColumn("type",  userDefinedFunctions.eaiBbhType.apply(functions.col("technology")))
                .withColumn("status", functions.lit("Unknown"))
                .select(b.col("name"), functions.col("type"), b.col("node_code"), b.col("id"), b.col("virtual_rnc"), functions.col("status"))
        );
        return joinBsAndn(b2, n);
    }

    public Dataset generateEnm(Dataset gnbd_e,Dataset nb_e,Dataset enb_e) {
        Dataset gnb_e = gnbd_e;
        gnb_e = gnb_e.distinct();
        gnb_e = transformGnbE(gnb_e);
        Dataset enm = nb_e;
     //   enm.show();//|mecontext|
     //   enb_e.show();//|mecontext|  id|


        enm = enm.union(enb_e);
        System.out.println("000000000000000000000");
        enm.show();
        gnb_e.show();
        return enm.union(gnb_e);
    }

    public Dataset transformNodeB(Dataset mbs) {
       return mbs.where(mbs.col("type").equalTo("ocw/nodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("nodeBId"), mbs.col("name"), mbs.col("status"));
    }

    public Dataset transformENodeB(Dataset mbs) {
       return mbs.where(mbs.col("type").equalTo("ocw/eNodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("eNodeBId"), mbs.col("name"), mbs.col("status"));
    }

    public Dataset transformgNBDU(Dataset mbs) {
       return mbs.where(mbs.col("type").equalTo("ocw/gnbdu")).select(functions.col("$type"), functions.col("$refId"),
                functions.col("$action"), mbs.col("id").alias("gnbduId"), mbs.col("name"), mbs.col("status"));
    }
}
