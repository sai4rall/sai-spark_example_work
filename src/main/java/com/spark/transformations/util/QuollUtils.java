package com.spark.transformations.util;

import com.spark.transformations.config.QuollMapConstants;
import org.apache.log4j.Logger;
import scala.reflect.ClassTag;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Pattern;

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
    public static String getBbhType(String tech){
        if(tech ==null)
            return null;
        else{
            if(tech.indexOf("LTE")>=0){
                return "ocw/eNodeB;"
            }
            if(tech.indexOf("NR")>=0){
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

    public static String genEGNodeBName(Integer du, String site, String nid, String nodeCode):

    {
        if (du == null) {
            Pattern lte1DigitPattern = Pattern.compile("\\(LTE[1-9]\\"));//. represents single character
            boolean m1d = lte1DigitPattern.matcher(site).matches();
//TODO- DISCUSS LOGIC

            Pattern lte2DigitPattern = Pattern.compile("\\(LTE[1-9][0-9]\\"));
            boolean m2d = lte1DigitPattern.matcher(site).matches();
            return null;// temp

        } else {
            //TODO- DISCUSS LOGIC
            return nodeCode + String.valueOf(du);
        }


    }


    public static String  genNodeBName(String site,String nodeCode){
        try{
     // TODO need to discuss logic
            return null;
        }catch(Exception e){
            return null;
        }
    }



}
