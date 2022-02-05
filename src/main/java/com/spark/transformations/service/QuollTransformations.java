package com.spark.transformations.service;

import com.spark.transformations.config.Constants;
import com.spark.transformations.util.QuollUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class QuollTransformations implements  Transformation{
    QuollUtils quollUtils;
    public QuollTransformations(){
        quollUtils=new QuollUtils();
    }
    @Override
    public Dataset apply(Dataset qdf) {
//# Add in the sector
        Dataset<Row> q=qdf.select(qdf.col("*"),
                qdf.col("cell_name").substr(5,2).alias("utchar"),
                qdf.col("cell_name").substr(7,1).alias("sector"));


        q=q.drop("bts_instance_display_name","clli_number_bed","clli_number_hex",
                "do_rnc","multi_nids","nid","switch_number","xrtt_enabled","cell_fro_id","cid_hex","lac_hex","enbid_hex");


        q.where(q.col("base_station_name").like("%WIFI%")).write().mode("overwrite").json(Constants.WIFI_NNI_PATH);


        q=q.where(functions.not(q.col("base_station_name").like("%WIFI%")));

        UserDefinedFunction ea1sectorNumber=functions.udf((String s)->{
            System.out.println(s+"========");
            return  QuollUtils.genSectorNumber(s);
        }, DataTypes.IntegerType);
        q.show();
        //TODO SECTORNO fix with UDF
        q=q.withColumn("sectorNumber",ea1sectorNumber.apply(q.col("sector")));
        q.show();
        q=q.where(functions.not(q.col("cell_status").isin("Erroneous entry","Removed")));
        q.show();
        return q;
    }
}
