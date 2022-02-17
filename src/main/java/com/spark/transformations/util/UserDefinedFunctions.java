package com.spark.transformations.util;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType$;

public class UserDefinedFunctions {
    public static UserDefinedFunction eaisectorNumber = functions.udf((String s) -> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaiCellStatus = functions.udf((String s) -> QuollUtils.napCellStatus(s), DataTypes.StringType);
    public static UserDefinedFunction eaiStatus = functions.udf((String s) -> QuollUtils.mapStatus(s), DataTypes.StringType);
    public static UserDefinedFunction eaiCellType = functions.udf((String s) -> QuollUtils.mapCellType(s), DataTypes.StringType);
    public static UserDefinedFunction eaiCellFunction = functions.udf((String s) -> QuollUtils.mapCellFunction(s), DataTypes.StringType);
    public static UserDefinedFunction eaiRac = functions.udf((String s) -> QuollUtils.cleanRac(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaivalidMscNode = functions.udf((String s) -> QuollUtils.validateMscNode(s), DataTypes.StringType);
    public static UserDefinedFunction eaiBool = functions.udf((String s) -> QuollUtils.cleanBool(s), DataTypes.BooleanType);
    public static UserDefinedFunction eaiYN = functions.udf((String s) -> QuollUtils.cleanYN(s), DataTypes.StringType);
    public static UserDefinedFunction eaiRacVal = functions.udf((String s) -> QuollUtils.cleanRacVal(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaiInt = functions.udf((String s) -> QuollUtils.cleanInt(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaiTechnologyToType = functions.udf((String s) -> QuollUtils.technologyToType(s), DataTypes.StringType);
    public static UserDefinedFunction eaiAreaCode = functions.udf((String s) -> QuollUtils.mapAreaCode(s), DataTypes.StringType);
    public static UserDefinedFunction eaiUra = functions.udf((String s) -> QuollUtils.cleanUra(s),DataTypes.IntegerType);
    public static UserDefinedFunction eaiChannel = functions.udf((String s) -> QuollUtils.cleanTechnology(s),DataTypes.IntegerType);
    public static UserDefinedFunction eaiNodeBName = functions.udf((String s1,String s2) -> QuollUtils.genNodeBName(s1,s2),DataTypes.StringType);
    public static UserDefinedFunction eaiBbhType = functions.udf((String s) -> QuollUtils.getBbhType(s),DataTypes.StringType);
    public static UserDefinedFunction eaiEGNodeBName = functions.udf((Integer i1,String s1,String s2,String s3) -> QuollUtils.genEGNodeBName(i1,s1,s2,s3),DataTypes.StringType);
    public static UserDefinedFunction eaiNameFromMecontext = functions.udf((String s1,Boolean b1) -> QuollUtils.extractNameFromMecontext(s1,b1),DataTypes.StringType);
    public static UserDefinedFunction eaiIdFromMecontext = functions.udf((String s1) -> QuollUtils.extractIdFromMecontext(s1),DataTypes.IntegerType);
    public static UserDefinedFunction eaiEnmGnbType = functions.udf((String s1) -> QuollUtils.enmGnbType(s1),DataTypes.StringType);




}
