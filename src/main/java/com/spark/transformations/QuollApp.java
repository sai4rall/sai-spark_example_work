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

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession session = ClusterConfig.getSparkSession();
        QuollUtils quollUtils = new QuollUtils();

        //TODO need to change to database
        Dataset qdf = quollUtils.readFile(session, null, "src/test/resources/in/sample_data.csv");
        Dataset q = quollUtils.applyInitialTrabsformaions(qdf);
        Dataset t = quollUtils.readFile(session, QuollSchemas.nodeIdSchema, Constants.TEMPEST_NODE_ID_PATH);
        Dataset b = quollUtils.readFile(session, QuollSchemas.bbhSpreadsheetSchema, Constants.TEMPEST_NODE_ID_PATH);


        q = quollUtils.addAdditionalAttributes(q);

        Dataset sites = quollUtils.cleanlyConvertssitesToInteger(q);


        sites.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site");

        Dataset bsc = quollUtils.transformBsc(q);

        bsc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");

        Dataset rnc = quollUtils.transformRnc(q);
        rnc.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc");


        Dataset bts = quollUtils.transfromBts(q);
        bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts");

        Dataset bsc_to_bts_lookup = quollUtils.transfromBscToBtsLookup(q);


        bsc_to_bts_lookup.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts_lookup");


//           # now get the list of the bts's that will link to them
        Dataset bsc_to_bts = quollUtils.transfromBscToBts(q);

//            #bsc_to_bts.show()
        bsc_to_bts.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bsc_to_bts");

        Dataset lte = quollUtils.transformLte(q);
        lte.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "lteCell");


        Dataset gsm = quollUtils.transformGsm(q);

        gsm.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gsmCell");


        Dataset umts = quollUtils.transformUmts(q);


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


        Dataset site_to_rfCell = quollUtils.transformSiteToRfCell(q);


//#site_to_rfCell.show()
        site_to_rfCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "site_to_rfCell");


        Broadcast ranNumberingMap = session.sparkContext().broadcast(QuollMapConstants.ranNumberingDict, QuollUtils.classTag(Map.class));

        Dataset bs = quollUtils.generateBsDataset(t,b);

//# n.count = 42749
//# b.count = 5698
//#n.count()
//#print(bs.count())       # 44425
//#bs.select('type').distinct().show()
//#bs.orderBy(bs.name).coalesce(1).write.csv(path='s3://emrdisco/eai_objects/baseStation/csv', mode='overwrite', header=True, quoteAll=True)
//#bs.write.json(path='s3://emrdisco/eai_objects/baseStation', mode='overwrite')
        Dataset nb_e =quollUtils.readFile(session,QuollSchemas.enmNodeBSchema,Constants.enm_nodeB_PATH);


        nb_e = quollUtils.transformnbe(nb_e);

        Dataset enb_e = quollUtils.readFile(session,QuollSchemas.enmBaseStationSchema,Constants.enm_nodeBS_PATH);

        enb_e = quollUtils.transformenbE(enb_e);
        Dataset gnbd_e = quollUtils.readFile(session,QuollSchemas.enmBaseStationSchema,Constants.GNODEB_DU);

        gnbd_e = quollUtils.transformGnbdE(gnbd_e);

        Dataset enm = quollUtils.generateEnm(gnbd_e,nb_e,enb_e);
//        #enm = enm.union(bts.select(bts.name, bts.btsId.alias('id'), bts.type, bts.status, bts.name.alias('nodeCode')))
        Dataset b3 = quollUtils.transformBsToB3(bs);

        Dataset mbs = quollUtils.transformenmToMbs(enm, b3, bs);

//#mbs.orderBy(mbs.name).show()

//# convert all the statuses from Tempest, BBH XLSX and ENM into valid ENM statuses values
//# TPD-1275 and TPD-1328

        mbs = quollUtils.transformMbs(mbs);

//# bs gives a consolidated list of base stations from TEMPEST and BBH XLSX
//#   we now need to split these up again so that we can add in type specific fields
//#   and also tweak the fields we display

        Dataset nodeB = quollUtils.transformNodeB(mbs);


        Dataset eNodeB = quollUtils.transformENodeB(mbs);

        Dataset gNBDU = quollUtils.transformgNBDU(mbs);


        nodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "nodeB");
        eNodeB.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "eNodeB");
        gNBDU.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "gNB-DU");
//#gNBCUUP.write.json(path=bucketUrl + bucketOutputPath + 'gNB-CU-UP', mode='overwrite')

        Dataset bts_to_gsmCell_lookup = quollUtils.transformBtsToGsmCelLoopUP(bts);
//#bts_to_gsmCell_lookup.show()
        bts_to_gsmCell_lookup.write().mode("overwrite")
                .json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell_lookup");

//# backup :   (q.iub_rbsid.isNotNull()) & (q.technology.like('GSM%')) & (q.cell_status != 'Erroneous entry')
        Dataset bts_to_gsmCell = quollUtils.transformBtsToGsmCell(q);

//#bts_to_gsmCell.show(50)
        bts_to_gsmCell.write().mode("overwrite").json(Constants.bucketUrl + Constants.bucketOutputPath + "bts_to_gsmCell");
    }
}
