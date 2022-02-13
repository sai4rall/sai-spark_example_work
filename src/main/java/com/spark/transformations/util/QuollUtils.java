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

        UserDefinedFunction ea1sectorNumber = functions.udf((String s) -> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);

        q.show();
        //TODO SECTORNO fix with UDF
        q = q.withColumn("sectorNumber", ea1sectorNumber.apply(q.col("sector")));
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
                        functions.col("|telstraCellAttributes|hasSpecialEvent"),
                        functions.col("|telstracellAttributes|hassignificantSpecialEvent"),
//                        functions.col("|telstracellattributes|mobileswitchingCentre"),//TODO need to check why this is not there
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
        return QuollMapConstants.statusMapDict.get(Status) != null ? QuollMapConstants.cellStatusMapDict.get(Status) : QuollMapConstants.cellStatusMapDict.get("Unknown");
    }

    public static String mapCellType(String qType) {
//          # note: we cannot have a status on None as the record will not load into EAI
        return QuollMapConstants.cellTypeMapDict.get(qType) != null ? QuollMapConstants.cellTypeMapDict.get(qType) : QuollMapConstants.cellStatusMapDict.get("TBA");
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
            else if (sTech.indexOf("GSH") > -1)
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


}
