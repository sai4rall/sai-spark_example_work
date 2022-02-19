package com.spark.transformations.util;

import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollMapConstants;
import com.spark.transformations.config.QuollSchemas;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

public class QuollUtils {
    static Logger logger = Logger.getLogger(QuollUtils.class);

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
        qdf.show();
        Dataset<Row> q = qdf.select(qdf.col("*"),
                qdf.col("cell_name").substr(5, 2).alias("utchar"),
                qdf.col("cell_name").substr(7, 1).alias("sector"));


        q = q.drop("bts_instance_display_name", "clli_number_bed", "clli_number_hex",
                "do_rnc", "multi_nids", "nid", "switch_number", "xrtt_enabled", "cell_fro_id", "cid_hex", "lac_hex", "enbid_hex");


        q.where(q.col("base_station_name").like("%WIFI%")).write().mode("overwrite").json(Constants.WIFI_NNI_PATH);


        q = q.where(functions.not(q.col("base_station_name").like("%WIFI%")));

//        UserDefinedFunction ea1sectorNumber = functions.udf((String s) -> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);

        q.show();
        q = q.withColumn("sectorNumber", UserDefinedFunctions.eaisectorNumber.apply(q.col("sector")));
        q.show();
        q = q.where(functions.not(q.col("cell_status").isin("Erroneous entry", "Removed")));
        q.show();
        return q;
    }


    public Dataset addAdditionalAttributes(Dataset q) {
        return q.withColumn("|telstraCellAttributes|cellFunction", UserDefinedFunctions.eaiCellFunction.apply(q.col("cell_function"))).
                withColumn("|telstraCellAttributes|hasSpecialEvent", UserDefinedFunctions.eaiBool.apply(q.col("special_event_cell"))).
                withColumn("|telstraCellAttributes|hasSignificantSpecialEvent", UserDefinedFunctions.eaiBool.apply(q.col("special_event"))).
                withColumn("|telstraCellAttributes|hasPriorityAssistCustomers", UserDefinedFunctions.eaiBool.apply(q.col("priority_assist"))).
                withColumn("|telstraCellAttributes|hasHighSeasonality", UserDefinedFunctions.eaiBool.apply(q.col("high_seasonality"))).
                withColumn("|telstraCellAttributes|hasWirelessLocalLoopCustomers", UserDefinedFunctions.eaiBool.apply(q.col("wll"))).
                withColumn("|telstraCellAttributes|mobileSwitchingCentre", UserDefinedFunctions.eaivalidMscNode.apply(q.col("msc_node"))).
                withColumn("|telstraCellAttributes|mobileServiceArea", q.col("msa")).
                withColumn("|telstraCellAttributes|quollIndex", UserDefinedFunctions.eaiInt.apply(q.col("cell_index"))).
                withColumn("|telstraCellAttributes|closedNumberArea", UserDefinedFunctions.eaiAreaCode.apply(q.col("cna")))
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
    public static Integer genSectorNumber(String sector) {

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


    public static String napCellStatus(String qStatus) {
//            # note: we cannot have a status on None as the record will not load into EAI
        return QuollMapConstants.cellStatusMapDict.get(qStatus) != null ? QuollMapConstants.cellStatusMapDict.get(qStatus) : QuollMapConstants.cellStatusMapDict.get("Concept");
    }

    public static String mapStatus(String Status) {
//          # note: we cannot have a status on None as the record will not load into EAI
        return QuollMapConstants.statusMapDict.get(Status) != null ? QuollMapConstants.statusMapDict.get(Status) : QuollMapConstants.statusMapDict.get("Unknown");
    }

    public static String mapCellType(String qType) {
//          # note: we cannot have a status on None as the record will not load into EAI
        return QuollMapConstants.cellTypeMapDict.get(qType) != null ? QuollMapConstants.cellTypeMapDict.get(qType) : QuollMapConstants.cellTypeMapDict.get("TBA");
    }

    public static String mapCellFunction(String qFunction) {
        if (qFunction != null && isInteger(qFunction)) {
            int temp = Integer.parseInt(qFunction);
            return QuollMapConstants.cellFunctionbict.get(temp);
        } else {
            return null;
        }
    }

    //    # use eaiInt instead
//# convert RAC to an int
    public static Integer cleanRac(String qrac) {
        if (qrac != null && isInteger(qrac)) {
            return Integer.parseInt(qrac);
        } else {
            return null;
        }
    }

    public static String validateMscNode(String node) {
        return node != null ? QuollMapConstants.validiscNodeDict.get(node) : null;
    }

    //        # convert a string flag into a Boolean
    public static boolean cleanBool(String qVal) {
        if (qVal != null &&
                (qVal.toUpperCase().equalsIgnoreCase("TRUE") ||
                        (qVal.toUpperCase().equalsIgnoreCase("SIGNIFICANT")))) {
            return true;
        } else {
            return false;
        }
    }

    public static String cleanYN(String qVal) {
        if (qVal != null &&
                (qVal.toUpperCase().equalsIgnoreCase("TRUE"))) {
            return "YES";

        } else {
            return "NO";
        }
    }

    //    # the LAC needs to be converted to Int in the range 1->65,535. Values of are to be changed to None.
    public static Integer cleanLac(String qval) {
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
    public static Integer cleanRacVal(String qval) {
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

    public static Integer cleanInt(String qval) {
        if (qval != null && isInteger(qval)) {
            return Integer.parseInt(qval);
        } else {
            logger.info("Error converting" + qval + "to int");
            return null;
        }
    }

    public static String mapAreaCode(String qAc) {
        if (qAc != null && isInteger(qAc)) {
            Integer ac = Integer.parseInt(qAc);
            return QuollMapConstants.areaCodeDict.get(ac);
        } else {
            logger.info("Error converting area code" + qAc);
            return null;
        }
    }


    public static Integer cleanUra(String qStr) {
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

    public static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    public static boolean isInteger(String s) {
        try {
            Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        } catch (NullPointerException e) {
            return false;
        }
        // only got here if we didn't return false
        return true;
    }

    public static String technologyToType(String sTech) {
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

    public static String getBbhType(String tech) {
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


    public static Integer cleanTechnology(String qStr) {
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
//TODO need to verify datatypes of parameters

    public static String genEGNodeBName(Integer du, String site, String nid, String nodeCode) {
        if (du == null) {
            //  Pattern lte1DigitPattern = Pattern.compile("\\(LTE[1-9]\\");//. represents single character
            //   boolean m1d = lte1DigitPattern.matcher(site).matches();
//TODO- DISCUSS LOGIC

            //   Pattern lte2DigitPattern = Pattern.compile("\\(LTE[1-9][0-9]\\"));
            //   boolean m2d = lte1DigitPattern.matcher(site).matches();
            return null;// temp

        } else {
            //TODO- DISCUSS LOGIC
            return nodeCode + String.valueOf(du);
        }


    }


    public static String genNodeBName(String site, String nodeCode) {
        try {
            // TODO need to discuss logic
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public static String extractNameFromMecontext(String qStr, Boolean paddOne) {
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

    public static Integer extractIdFromMecontext(String qStr) {
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

    public static String enmGnbType(String mecontext) {
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
                .withColumn("status", UserDefinedFunctions.eaiCellStatus.apply(functions.col("cell_status")))                        //       # ocw:telstraWirelessDeploymentStatusPicklist
                .withColumn("cellType", UserDefinedFunctions.eaiCellType.apply(functions.col("base_station_type")))                     //# ocw:telstraCellTypePicklist
//                 .withColumn("lac", eaiLac(functions.col("lac_dec")))
                .withColumn("rac", UserDefinedFunctions.eaiRac.apply(functions.col("rac_dec")))
                .withColumn("ura", UserDefinedFunctions.eaiUra.apply(functions.col("ura")))
                .withColumn("trackingAreaCode", UserDefinedFunctions.eaiInt.apply(functions.col("tac")))                              //  # Convert string to int via udf
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
                .withColumn("$type", UserDefinedFunctions.eaiTechnologyToType.apply(functions.col("technology")))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .withColumn("$site", functions.array(functions.col("base_station_name")))
                .select(functions.col("$type"), functions.col("$action"), q.col("cell_name").alias("$refId"),
                        functions.col("$site"), q.col("cell_name").alias("name")
                )
        );
    }

    public Dataset transform4GLRAN(Dataset t) {
        return (t.where(t.col("network").isin("4G (LRAN)").and(t.col("rbs_id").isNotNull()))
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
    }

    public Dataset transform5GNGRAN(Dataset t) {
        return (t.where(t.col("network").isin("5G (NGRAN)").and(t.col("gnb_id").isNotNull()))
                .withColumn("name", UserDefinedFunctions.eaiEGNodeBName.apply(functions.col("du_number"), functions.col("site_name"), functions.col("gnb_id"), functions.col("node_code")))
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
        (t.where(t.col("network").isin("3G (WRAN)").and(t.col("rbs_id").isNotNull()))).show();
        return  (t.where(t.col("network").isin("3G (WRAN)").and(t.col("rbs_id").isNotNull()))
                .withColumn("name", UserDefinedFunctions.eaiNodeBName.apply(functions.col("site_name"), functions.col("node_code")))
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
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("id", UserDefinedFunctions.eaiIdFromMecontext.apply(functions.col("mecontext")))
                .withColumn("type", functions.lit("ocw/nodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformenbE(Dataset enb_e) {
        return (enb_e
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .withColumn("type", functions.lit("ocw/eNodeB"))
                .withColumn("status", functions.lit("In Service"))
                .select(functions.col("name"), functions.col("id"), functions.col("type"), functions.col("status"),
                        functions.substring(functions.col("name"), 1, 4).alias("nodeCode"))
                .where(functions.col("id").isNotNull())
        );
    }

    public Dataset transformGnbdE(Dataset gnbd_e) {
        return  (gnbd_e
                .withColumn("name", UserDefinedFunctions.eaiNameFromMecontext.apply(functions.col("mecontext"), functions.lit(true)))
                .select(functions.col("name"), functions.col("id"), gnbd_e.col("mecontext"))
                .where(functions.col("id").isNotNull()));
    }

    public Dataset transformGnbE(Dataset gnb_e) {
       return  (gnb_e
                .withColumn("type", UserDefinedFunctions.eaiEnmGnbType.apply(functions.col("mecontext")))
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
                .withColumn("status", UserDefinedFunctions.eaiStatus.apply(functions.col("tmp")))
                .withColumn("$refId", functions.col("name"))
                .withColumn("$type", functions.col("type"))
                .withColumn("$action", functions.lit("createOrUpdate"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id"), mbs.col("name"), mbs.col("type"), functions.col("status")
                        , mbs.col("nodeCode"))
        );
    }
}
