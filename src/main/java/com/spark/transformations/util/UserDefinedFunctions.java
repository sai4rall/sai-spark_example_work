package com.spark.transformations.util;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class UserDefinedFunctions {
    public static UserDefinedFunction ea1sectorNumber= functions.udf((String s)-> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaicelistatus= functions.udf((String s)-> QuollUtils.napCellStatus(s), DataTypes.StringType);
    public static UserDefinedFunction ealStatus= functions.udf((String s)-> QuollUtils.mapStatus(s), DataTypes.StringType);
    public static UserDefinedFunction eaiCellType= functions.udf((String s)-> QuollUtils.mapCellType(s), DataTypes.StringType);
    public static UserDefinedFunction eaiCellFunction= functions.udf((String s)-> QuollUtils.mapCellFunction(s), DataTypes.StringType);
    public static UserDefinedFunction eaiRac= functions.udf((String s)-> QuollUtils.cleanRac(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaivalidMscNode= functions.udf((String s)-> QuollUtils.validateMscNode(s), DataTypes.StringType);
    public static UserDefinedFunction eaiBool= functions.udf((String s)-> QuollUtils.cleanBool(s), DataTypes.BooleanType);
    public static UserDefinedFunction eaiYN= functions.udf((String s)-> QuollUtils.cleanYN(s), DataTypes.StringType);
    public static UserDefinedFunction eaiRacVal= functions.udf((String s)-> QuollUtils.cleanRacVal(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaiInt= functions.udf((String s)-> QuollUtils.cleanInt(s), DataTypes.IntegerType);
    public static UserDefinedFunction eaiTechnologyToType= functions.udf((String s)-> QuollUtils.technologyToType(s), DataTypes.StringType);
    public static UserDefinedFunction eaiAreaCode= functions.udf((String s)-> QuollUtils.mapAreaCode(s), DataTypes.StringType);
}
