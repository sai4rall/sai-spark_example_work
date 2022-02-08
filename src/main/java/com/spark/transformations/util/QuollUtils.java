package com.spark.transformations.util;

import com.spark.transformations.config.QuollMapConstants;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Locale;

public class QuollUtils {
    static Logger logger = Logger.getLogger(QuollUtils.class);

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


    public static Integer cleanUra(String qStr){
        if(qStr==null)
        {
            return null;
        }else{
            if(isInteger(qStr.split(":")[0])){
                return Integer.parseInt(qStr.split(":")[0]);
            }else{
                logger.warn("error cleaning URA field"+ qStr);
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


}
