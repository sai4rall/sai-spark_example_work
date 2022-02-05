package com.spark.transformations.util;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class UserDefinedFunctions {
    UserDefinedFunction ea1sectorNumber= functions.udf((String s)-> QuollUtils.genSectorNumber(s), DataTypes.IntegerType);
    UserDefinedFunction eaicelistatus= functions.udf((String s)-> QuollUtils.napCellStatus(s), DataTypes.StringType);
    UserDefinedFunction ealStatus= functions.udf((String s)-> QuollUtils.mapStatus(s), DataTypes.StringType);
    UserDefinedFunction eaiCellType= functions.udf((String s)-> QuollUtils.mapCellType(s), DataTypes.StringType);
    UserDefinedFunction eaiCellFunction= functions.udf((String s)-> QuollUtils.mapCellFunction(s), DataTypes.StringType);
    UserDefinedFunction eaiRac= functions.udf((String s)-> QuollUtils.cleanRac(s), DataTypes.IntegerType);

}
