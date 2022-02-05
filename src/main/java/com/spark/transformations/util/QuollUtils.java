package com.spark.transformations.util;

import scala.reflect.ClassTag;

import java.util.Arrays;

public class QuollUtils {
    public static Integer genSectorNumber(String sector) {
        System.out.println("hello world");
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

    public static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

}
