package com.spark.transformations.config;

import org.apache.spark.sql.types.*;

public class QuollSchemas {
    public static StructType nodeIdSchema = new StructType()
            .add("nodeb_allocation_id", IntegerType$.MODULE$, false)
            .add("network", StringType$.MODULE$, true)
            .add("address_id", IntegerType$.MODULE$, true)
            .add("node_code",StringType$.MODULE$, true)
            .add("du_number", IntegerType$.MODULE$, true)
            .add("site_name", StringType$.MODULE$, true)
            .add("mecontext_site_nane", StringType$.MODULE$, true)
            .add("remote_node_code", StringType$.MODULE$, true)
            .add("remote_site_name",StringType$.MODULE$, true)
            .add("network_type",StringType$.MODULE$, true)
            .add("nodeb_classification",StringType$.MODULE$, true)
            .add("rbs_id",IntegerType$.MODULE$, true)
            .add("status",StringType$.MODULE$, true)
            .add("gnb_id",IntegerType$.MODULE$, true)
            .add("gnb_status",StringType$.MODULE$, true)
            .add("neindex", StringType$.MODULE$, true)
            .add("host_state",StringType$.MODULE$, true)
            .add("virtual_rnc_type",StringType$.MODULE$, true)
            .add("virtual_rnc",StringType$.MODULE$,true)
            .add("rnc_module_Info",StringType$.MODULE$, true)
            .add("comments", StringType$.MODULE$, true)
            .add("allocated_by",StringType$.MODULE$, true)
            .add("allocation_date", StringType$.MODULE$, true)
            .add("requested_by", StringType$.MODULE$,true)
            .add("request_date",DateType$.MODULE$, true);

    public static StructType bbhSpreadsheetSchema = new StructType()
            .add("name",StringType$.MODULE$, false)
            .add("id", IntegerType$.MODULE$, false)
            .add("node_code", StringType$.MODULE$ ,false)
            .add("technology", StringType$.MODULE$, true)
            .add("virtual_rnc", StringType$.MODULE$, true)
            .add("filename", StringType$.MODULE$, true);
    public static StructType enmNodeBSchema = new StructType()
            .add("mecontext", DataTypes.StringType, false);

    public static StructType enmBaseStationSchema = new StructType()
            .add("mecontext", DataTypes.StringType, false)
            .add("id", DataTypes.IntegerType, false);
}
