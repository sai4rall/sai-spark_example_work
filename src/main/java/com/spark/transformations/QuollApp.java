package com.spark.transformations;

import com.spark.transformations.config.ClusterConfig;
import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollMapConstants;
import com.spark.transformations.config.QuollSchemas;
import com.spark.transformations.util.QuollUtils;
import com.spark.transformations.util.UserDefinedFunctions;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import java.util.Map;


public class QuollApp {
    public static void main(String[] args) {
        System.out.println("Hello ");

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = ClusterConfig.getSparkSession();
        QuollUtils quollUtils = new QuollUtils();

        //TODO need to change to database
        Dataset qdf = quollUtils.readFile(session, null, "src/test/resources/in/sample_data.csv");
        Dataset q = quollUtils.applyInitialTrabsformaions(qdf);
        Dataset t = quollUtils.readFile(session, QuollSchemas.nodeIdSchema, Constants.TEMPEST_NODE_ID_PATH);
        Dataset b = quollUtils.readFile(session, QuollSchemas.bbhSpreadsheetSchema, Constants.TEMPEST_NODE_ID_PATH);


        Broadcast cellStatusMap = session.sparkContext().broadcast(QuollMapConstants.cellStatusMapDict, QuollUtils.classTag(Map.class));
        Broadcast statusMapDict = session.sparkContext().broadcast(QuollMapConstants.statusMapDict, QuollUtils.classTag(Map.class));
        Broadcast cellTypeMap = session.sparkContext().broadcast(QuollMapConstants.cellTypeMapDict, QuollUtils.classTag(Map.class));
        Broadcast cellFunction = session.sparkContext().broadcast(QuollMapConstants.cellFunctionbict, QuollUtils.classTag(Map.class));
        Broadcast validscnodes = session.sparkContext().broadcast(QuollMapConstants.validiscNodeDict, QuollUtils.classTag(Map.class));
        Broadcast areaCode = session.sparkContext().broadcast(QuollMapConstants.areaCodeDict, QuollUtils.classTag(Map.class));


        q = quollUtils.addAdditionalAttributes(q);
//q.show();
        Dataset sites = quollUtils.cleanlyConvertssitesToInteger(q);

//sites.show();
        sites.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site");

        Dataset bsc = quollUtils.transformBsc(q);

        bsc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");
//                #bsc.show()

//        q.show();
        Dataset rnc = quollUtils.transformRnc(q);
        rnc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");
        rnc.show();


        Dataset bts = quollUtils.transfromBts(q);
        bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts");

        Dataset bsc_to_bts_lookup = quollUtils.transfromBscToBtsLookup(q);

        //bsc_to_bts_lookup.show()
        bsc_to_bts_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts_lookup");


//           # now get the list of the bts's that will link to them
        Dataset bsc_to_bts = quollUtils.transfromBscToBts(q);

//            #bsc_to_bts.show()
        bsc_to_bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts");

        Dataset lte = quollUtils.transformLte(q);
        lte.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell");


        Dataset gsm = quollUtils.transformGsm(q);

//        gsm.show(10, 0, true);
        gsm.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gsmCell");


        Dataset umts =  quollUtils.transformUmts(q);



        //#umts.show(vertical=True, truncate=False)
        //#umts.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/umtsCell/csv', mode='overwrite', header=True, quoteAll=True)
        umts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "umtsCell");



        Dataset nr = quollUtils.transformNr(q);


//#nr.show()
//#nr.coalesce(1).write.csv(path='s3://emrdisco/eai_objects/nrCell/csv', mode='overwrite', header=True, quoteAll=True)
        nr.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nrCell");


        Dataset site_to_rfCell_lookup = quollUtils.transformSiteToRfCellLookUp(q);

//#site_to_rfCell_lookup.show()
        site_to_rfCell_lookup.write().mode("overwrite").json(Constants.SITE_TO_RFCELL_LOOKUP_PATH);


        Dataset site_to_rfCell =  quollUtils.transformSiteToRfCell(q);


//#site_to_rfCell.show()
        site_to_rfCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site_to_rfCell");


        Broadcast ranNumberingMap = session.sparkContext().broadcast(QuollMapConstants.ranNumberingDict, QuollUtils.classTag(Map.class));


        Dataset n = quollUtils.transform4GLRAN(t);
        n = n.union(quollUtils.transform5GNGRAN(t));
        n = n.union(quollUtils.transform3GWRAN(t));




        Dataset b2 = (b
                .withColumn("type", UserDefinedFunctions.eaiBbhType.apply(functions.col("technology")))
                .withColumn("status", functions.lit("Unknown"))
                .select(b.col("name"), functions.col("type"), b.col("node_code"), b.col("id"), b.col("virtual_rnc"), functions.col("status"))
        );

        Dataset bs = (b2
                .join(n, (b2.col("name").equalTo(n.col("name"))), "inner")
                .select(b2.col("name"), b2.col("type"), b2.col("node_code"), b2.col("id"), b2.col("virtual_rnc"),
                        b2.col("status"))   //   # select the bbh side
                .union(b2.join(n, b2.col("name").equalTo(n.col("name")), "left_anti"))
                .union(n.join(b2, n.col("name").equalTo(b2.col("name")), "left_anti"))     //  # psudo right_anti
                .where(functions.col("name").isNotNull())
                .distinct());
//# n.count = 42749
//# b.count = 5698
//#n.count()
//#print(bs.count())       # 44425
//#bs.select('type').distinct().show()
//#bs.orderBy(bs.name).coalesce(1).write.csv(path='s3://emrdisco/eai_objects/baseStation/csv', mode='overwrite', header=True, quoteAll=True)
//#bs.write.json(path='s3://emrdisco/eai_objects/baseStation', mode='overwrite')
        Dataset nb_e = session.read().option("header", "true")
                .schema(QuollSchemas.enmNodeBSchema)
                .csv(Constants.enm_nodeB_PATH);


        nb_e =quollUtils.transformnbe(nb_e);

        Dataset enb_e = session.read().schema(QuollSchemas.enmBaseStationSchema)
                .option("header", "true").csv(Constants.enm_nodeBS_PATH);

        enb_e =quollUtils.transformenbE(enb_e);
        Dataset gnbd_e = session.read()
                .option("header", "true")
                .schema(QuollSchemas.enmBaseStationSchema)
                .csv(Constants.GNODEB_DU);

        gnbd_e = quollUtils.transformGnbdE(gnbd_e);

        Dataset gnb_e = gnbd_e;
        gnb_e = gnb_e.distinct();
        gnb_e = quollUtils.transformGnbE(gnb_e);
        Dataset enm = nb_e;
        enm = enm.union(enb_e);
        enm = enm.union(gnb_e);
//        #enm = enm.union(bts.select(bts.name, bts.btsId.alias('id'), bts.type, bts.status, bts.name.alias('nodeCode')))
        Dataset b3 = bs.select(bs.col("name").alias("bsname"), bs.col("id"), bs.col("type").alias("bstype"),
                bs.col("status").alias("bsstatus"), bs.col("node_code").alias("bsnodeCode"));

//       # For all of the id's that match ENM and SB, keep the ENM version
        Dataset tmp1 = enm.join(b3, functions.col("id"), "left_outer").select("id", "name", "type", "status", "nodeCode");   //  #for this join get the ENM side

//# get the remaining records that are in BS but not in ENM
        Dataset tmp3 = bs.join(enm, functions.col("id"), "left_anti").select(functions.col("id"), functions.col("name"), functions.col("type"), functions.col("status"), bs.col("node_code").alias("nodeCode"));

        Dataset mbs = tmp1.union(tmp3);

//#mbs.orderBy(mbs.name).show()

//# convert all the statuses from Tempest, BBH XLSX and ENM into valid ENM statuses values
//# TPD-1275 and TPD-1328
        mbs = quollUtils.transformMbs(mbs);


//# bs gives a consolidated list of base stations from TEMPEST and BBH XLSX
//#   we now need to split these up again so that we can add in type specific fields
//#   and also tweak the fields we display

        Dataset nodeB = mbs.where(mbs.col("type").equalTo("ocw/nodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("nodeBId"), mbs.col("name"), mbs.col("status"));
        Dataset eNodeB = mbs.where(mbs.col("type").equalTo("ocw/eNodeB"))
                .select(functions.col("$type"), functions.col("$refId"), functions.col("$action"),
                        mbs.col("id").alias("eNodeBId"), mbs.col("name"), mbs.col("status"));
        Dataset gNBDU = mbs.where(mbs.col("type").equalTo("ocw/gnbdu")).select(functions.col("$type"), functions.col("$refId"),
                functions.col("$action"), mbs.col("id").alias("gnbduId"), mbs.col("name"), mbs.col("status"));
//#gNBCUUP = mbs.where(mbs.type == 'ocw/gnbcuup').select('$type', '$refId', '$action', mbs.id.alias('gnbcuupId'), mbs.name, mbs.status)
//
//
//#print(nodeB.count())       # 14706
//#print(eNodeB.count())      # 25490
//#print(gNBDU.count())       # 1063
//#print(gNBCUUP.count())     # 3162
        nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB");
        eNodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB");
        gNBDU.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gNB-DU");
//#gNBCUUP.write.json(path=bucketUrl + bucketOutputPath + 'gNB-CU-UP', mode='overwrite')

        Dataset bts_to_gsmCell_lookup =quollUtils.transformBtsToGsmCelLoopUP(bts);
//#bts_to_gsmCell_lookup.show()
        bts_to_gsmCell_lookup.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell_lookup");

//# backup :   (q.iub_rbsid.isNotNull()) & (q.technology.like('GSM%')) & (q.cell_status != 'Erroneous entry')
        Dataset bts_to_gsmCell =  quollUtils.transformBtsToGsmCell(q);

//#bts_to_gsmCell.show(50)
        bts_to_gsmCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell");
    }
}
