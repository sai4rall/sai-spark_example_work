package com.spark.transformations.util;

import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;


public class UserDefinedFunctions implements Serializable {
    QuollUtils quollUtils;
    UserDefinedFunctions(QuollUtils quollUtils){
       this.quollUtils=quollUtils;
    }
    
    public UserDefinedFunction eaisectorNumber = functions.udf((String s) -> quollUtils.genSectorNumber(s), DataTypes.IntegerType);
    public UserDefinedFunction eaiCellStatus = functions.udf((String s) -> quollUtils.mapCellStatus(s), DataTypes.StringType);
    public UserDefinedFunction eaiStatus = functions.udf((String s) -> quollUtils.mapStatus(s), DataTypes.StringType);
    public UserDefinedFunction eaiCellType = functions.udf((String s) -> quollUtils.mapCellType(s), DataTypes.StringType);
    public UserDefinedFunction eaiCellFunction = functions.udf((String s) -> quollUtils.mapCellFunction(s), DataTypes.StringType);
    public UserDefinedFunction eaiRac = functions.udf((String s) -> quollUtils.cleanRac(s), DataTypes.IntegerType);
    public UserDefinedFunction eaivalidMscNode = functions.udf((String s) -> quollUtils.validateMscNode(s), DataTypes.StringType);
    public UserDefinedFunction eaiBool = functions.udf((String s) -> quollUtils.cleanBool(s), DataTypes.BooleanType);
    public UserDefinedFunction eaiYN = functions.udf((String s) -> quollUtils.cleanYN(s), DataTypes.StringType);
    public UserDefinedFunction eaiRacVal = functions.udf((String s) -> quollUtils.cleanRacVal(s), DataTypes.IntegerType);
    public UserDefinedFunction eaiInt = functions.udf((String s) -> quollUtils.cleanInt(s), DataTypes.IntegerType);
    public UserDefinedFunction eaiTechnologyToType = functions.udf((String s) -> quollUtils.technologyToType(s), DataTypes.StringType);
    public UserDefinedFunction eaiAreaCode = functions.udf((String s) -> quollUtils.mapAreaCode(s), DataTypes.StringType);
    public UserDefinedFunction eaiUra = functions.udf((String s) -> quollUtils.cleanUra(s),DataTypes.IntegerType);
    public UserDefinedFunction eaiChannel = functions.udf((String s) -> quollUtils.cleanTechnology(s),DataTypes.IntegerType);
    public UserDefinedFunction eaiNodeBName = functions.udf((String s1,String s2) -> quollUtils.genNodeBName(s1,s2),DataTypes.StringType);
    public UserDefinedFunction eaiBbhType = functions.udf((String s) -> quollUtils.getBbhType(s),DataTypes.StringType);
    public UserDefinedFunction eaiEGNodeBName = functions.udf((String du,String site,Integer nid,String nodeCode) -> quollUtils.genEGNodeBName(du,site,nid,nodeCode),DataTypes.StringType);
    public UserDefinedFunction eaiNameFromMecontext = functions.udf((String s1,Boolean b1) -> quollUtils.extractNameFromMecontext(s1,b1),DataTypes.StringType);
    public UserDefinedFunction eaiIdFromMecontext = functions.udf((String s1) -> quollUtils.extractIdFromMecontext(s1),DataTypes.IntegerType);
    public UserDefinedFunction eaiEnmGnbType = functions.udf((String s1) -> quollUtils.enmGnbType(s1),DataTypes.StringType);




}
