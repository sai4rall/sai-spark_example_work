package com.spark.transformations.util;

import com.spark.transformations.config.QuollMapConstants;
import scala.reflect.ClassTag;

import java.util.Arrays;

public class QuollUtils {
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

    public String validateMscNode(String node) {
        return node != null ? QuollMapConstants.validiscNodeDict.get(node) : null;
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
}
