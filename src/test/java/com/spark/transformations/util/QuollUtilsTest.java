package com.spark.transformations.util;

import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollSchemas;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QuollUtilsTest {
    SparkSession session ;
    QuollUtils quollUtils;
    Dataset testDataset;
    Dataset tespest;
    Dataset nb_e;
    @BeforeEach
    void setUp() {
        session = SparkSession.builder().appName("QuollTransformationsTest")
                .master("local[1]")
                .getOrCreate();
        quollUtils = new QuollUtils();
        testDataset = quollUtils.readFile(session, null, "src/test/resources/testoneinput.csv");
        tespest = quollUtils.readFile(session,  QuollSchemas.nodeIdSchema, "src/test/resources/in/TEMPEST.csv");
        nb_e = quollUtils.readFile(session,  QuollSchemas.enmNodeBSchema, "src/test/resources/in/testdata2.csv");

    }

    @AfterEach
    void tearDown() {
        session.stop();
    }

    @Test
    void readFile() {
        Dataset q = quollUtils.readFile(session, null, "src/test/resources/in/TEMPEST.csv");
        assertEquals(3, q.count());
        Dataset t = quollUtils.readFile(session, QuollSchemas.nodeIdSchema, Constants.TEMPEST_NODE_ID_PATH);
        assertEquals(3, t.count());

    }

    @Test
    void applyInitialTrabsformaions() {
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        assertEquals(6, q.count());

    }

    @Test
    void cleanlyConvertssitesToInteger() {
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset sites = quollUtils.cleanlyConvertssitesToInteger(q);
        assertEquals(5, sites.count());
    }
    @Test
    void transformBsc(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset bscdata = quollUtils.transformBsc(q);
        assertEquals(1, bscdata.count());

    }
    @Test
    public void transformRnc(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset bscdata = quollUtils.transformBsc(q);
        assertEquals(1, bscdata.count());
    }
    @Test
    void transfromBts(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset bscdata = quollUtils.transfromBts(q);
        assertEquals(3, bscdata.count());
    }
    @Test
    void transfromBscToBtsLookup(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset btsLookup = quollUtils.transfromBscToBtsLookup(q);
        assertEquals(1, btsLookup.count());
    }
    @Test
void transfromBscToBts(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset btsLookup = quollUtils.transfromBscToBts(q);
        assertEquals(3, btsLookup.count());
}

    @Test
    void transformLte() {
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset lteDs = quollUtils.transformLte(q);
        assertEquals(1, lteDs.count());
    }

    @Test
    void transformGsm() {// TODO not done
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset gsmDs = quollUtils.transformGsm(q);
        assertEquals(3, gsmDs.count());
    }
@Test
void transformUmts(){
    Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
    q = quollUtils.addAdditionalAttributes(q);
    Dataset umtsDs = quollUtils.transformUmts(q);
    assertEquals(1, umtsDs.count());
}

    @Test
    void transformNr(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset nrDs = quollUtils.transformNr(q);
        assertEquals(1, nrDs.count());
    }
    @Test
    void transformSiteToRfCellLookUp(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset sitesToRfCellLookupDs = quollUtils.transformSiteToRfCellLookUp(q);
        assertEquals(5, sitesToRfCellLookupDs.count());
    }

    @Test
    void transformSiteToRfCell(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset sitesToRfCellDs = quollUtils.transformSiteToRfCell(q);
        assertEquals(6, sitesToRfCellDs.count());
    }

    @Test
    void transform4GLRAN(){
        Dataset tempestDataset = quollUtils.transform4GLRAN(tespest);
        assertEquals(1, tempestDataset.count());

    }
@Test
void  transform5GNGRAN() {
    Dataset transformed5GDataset = quollUtils.transform5GNGRAN(tespest);
    assertEquals(1, transformed5GDataset.count());
}
@Test
void transformnbe(){

    Dataset transformedmnbe = quollUtils.transformnbe(nb_e);

    assertEquals(4, transformedmnbe.count());
}
@Test
    void transformenbE(){
        Dataset enb_e = session.read().schema(QuollSchemas.enmBaseStationSchema)
                .option("header", "true").csv("src/test/resources/in/enm/enm_nodeB.csv");
        assertEquals(0,quollUtils.transformenbE(enb_e).count());
    }
    @Test
    void transformGnbdE(){
       Dataset gnbd_e = session.read()
                .option("header", "true")
                .schema(QuollSchemas.enmBaseStationSchema)
                .csv("src/test/resources/in/enm/enm_gNodeB-DU.csv");

        assertEquals(4,quollUtils.transformGnbdE(gnbd_e).count());
    }
    @Test
    void transformGnbE(){
        Dataset gnbd_e = session.read()
                .option("header", "true")
                .schema(QuollSchemas.enmBaseStationSchema)
                .csv("src/test/resources/in/enm/enm_gNodeB-DU.csv");
gnbd_e=quollUtils.transformGnbdE(gnbd_e);
        assertEquals(4,quollUtils.transformGnbE(gnbd_e).count());

    }
    @Test
    public void transformBtsToGsmCell(){
        Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
        q = quollUtils.addAdditionalAttributes(q);
        Dataset gsmBTsCell = quollUtils.transformBtsToGsmCell(q);
        assertEquals(3, gsmBTsCell.count());
    }

@Test
void transformBtsToGsmCelLoopUP(){
    Dataset q = quollUtils.applyInitialTrabsformaions(testDataset);
    Dataset bts = quollUtils.transfromBts(q);
    Dataset brsGsm = quollUtils.transformBtsToGsmCelLoopUP(bts);
    assertEquals(3, brsGsm.count());
}
@Test
public void transformMbs() {

    }
@Test
void transform3GWRAN(){
tespest.show();
    Dataset transformed5GDataset = quollUtils.transform3GWRAN(tespest);
    assertEquals(1, transformed5GDataset.count());
}
    @Test
    void genSectorNumber() {
        assertEquals(1,QuollUtils.genSectorNumber("1"));
        assertEquals(null,QuollUtils.genSectorNumber("  "));
        assertEquals(null,QuollUtils.genSectorNumber(" b "));
        assertEquals(null,QuollUtils.genSectorNumber(null));

    }


    @Test
    void napCellStatus() {
        assertEquals("PENDING DECOMMISSION",QuollUtils.napCellStatus("Inactive"));
        assertEquals("IN CONCEPT",QuollUtils.napCellStatus("xyz"));
        assertEquals("IN CONCEPT",QuollUtils.napCellStatus("Concept"));

    }

    @Test
    void mapStatus() {
        assertEquals("PENDING",QuollUtils.mapStatus("Commissioning"));
        assertEquals("PENDING",QuollUtils.mapStatus("xyz"));
        assertEquals("PENDING DECOMMISSION",QuollUtils.mapStatus("Pending Delete"));

    }

    @Test
    void mapCellType() {
        assertEquals("MACRO",QuollUtils.mapCellType("xyz"));
        assertEquals("MACRO",QuollUtils.mapCellType("TBA"));
        assertEquals("IN BUILDING CELL",QuollUtils.mapCellType("IBC"));

    }


    @Test
    void mapCellFunction() {
        assertEquals("Coverage",quollUtils.mapCellFunction("1"));
        assertEquals(null,quollUtils.mapCellFunction("1a"));
    }

    @Test
    void cleanRac() {
        assertEquals(1,quollUtils.cleanRac("1"));

    }

    @Test
    void validateMscNode() {
        assertEquals("NSW-ACT-HSCPOOL",quollUtils.validateMscNode("NSW-ACT-HSCPOOL"));

    }

    @Test
    void cleanBool() {
        assertTrue(quollUtils.cleanBool("TRUE"));
        assertTrue(quollUtils.cleanBool("SIGNIFICANT"));
        assertFalse(quollUtils.cleanBool("test"));
    }

    @Test
    void cleanYN() {
        assertEquals("YES",quollUtils.cleanYN("TRUE"));
        assertEquals("NO",quollUtils.cleanYN("NO"));
        assertEquals("NO",quollUtils.cleanYN("OTHER"));

    }

    @Test
    void cleanLac() {
        assertEquals(200,quollUtils.cleanLac("200"));
        assertEquals(null,quollUtils.cleanLac("-1"));
        assertEquals(null,quollUtils.cleanLac("abc"));

    }

    @Test
    void cleanRacVal() {
        assertEquals(254,quollUtils.cleanRacVal("254"));
        assertEquals(null,quollUtils.cleanRacVal("256"));
        assertEquals(null,quollUtils.cleanRacVal("abc"));

    }

    @Test
    void cleanInt() {
        assertEquals(254,quollUtils.cleanInt("254"));
        assertEquals(null,quollUtils.cleanInt("2aa54"));

    }

    @Test
    void mapAreaCode() {

        assertEquals("02",quollUtils.mapAreaCode("2"));
        assertEquals(null,quollUtils.mapAreaCode("abc"));

    }

    @Test
    void cleanUra() {
        assertEquals(2,quollUtils.cleanUra("2:45"));

    }

    @Test
    void classTag() {
    }

    @Test
    void isInteger() {
        assertTrue(quollUtils.isInteger("2"));
        assertFalse(quollUtils.isInteger("2a"));

    }

    @Test
    void technologyToType() {
        assertEquals("On/nrCell",quollUtils.technologyToType("NR"));
        assertEquals("ocw/lteCell",quollUtils.technologyToType("LTE"));
        assertEquals("Ocw/umtsCell",quollUtils.technologyToType("WCDMA"));
        assertEquals("ocw/gsmCell",quollUtils.technologyToType("GSM"));
        assertEquals(null,quollUtils.technologyToType(""));
        assertEquals(null,quollUtils.technologyToType("other"));

    }

    @Test
    void getBbhType() {
        assertEquals("ocw/eNodeB",quollUtils.getBbhType("LTE"));
        assertEquals("ocw/gnbdu",quollUtils.getBbhType("NR"));
        assertEquals(null,quollUtils.getBbhType(null));
        assertEquals(null,quollUtils.getBbhType("other"));

    }


    @Test
    void cleanTechnology() {
        assertEquals(26000,quollUtils.cleanTechnology("NR26G"));
        assertEquals(null,quollUtils.cleanTechnology(null));
        assertEquals(null,quollUtils.cleanTechnology("other nonintval"));

    }



    @Test
    void extractNameFromMecontext() {
        assertEquals(null, quollUtils.extractNameFromMecontext("test",null));
        assertEquals(null, quollUtils.extractNameFromMecontext(null,false));
        assertEquals(null, quollUtils.extractNameFromMecontext(null,false));
        assertEquals("est", quollUtils.extractNameFromMecontext("test_est",true));
        assertEquals(null, quollUtils.extractNameFromMecontext("testeetest",true));

    }

    @Test
    void extractIdFromMecontext() {
        assertEquals(null, quollUtils.extractIdFromMecontext("test_test"));
        assertEquals(null, quollUtils.extractIdFromMecontext(null));
        assertEquals(123, quollUtils.extractIdFromMecontext("test_123"));

    }

    @Test
    void enmGnbType() {
        assertEquals("ocw/gnbdu", quollUtils.enmGnbType("TEST_BBH_testvak"));
        assertEquals("ocw/gnbdu", quollUtils.enmGnbType("TESTtestvak"));
        assertEquals(null, quollUtils.enmGnbType(null));

    }
    @Test
    void genEGNodeBName(){

        assertEquals("NODECODEtESTdutest",quollUtils.genEGNodeBName("dutest", "(LTE3)", 50000, "NODECODEtEST"));
        assertEquals("NODECODEtEST30",quollUtils.genEGNodeBName(null, "(LTE3)", 50000, "NODECODEtEST"));
        assertEquals("NODECODEtESTZ0",quollUtils.genEGNodeBName(null, "(LTE33)", 50000, "NODECODEtEST"));
        assertEquals("NODECODEtEST10",quollUtils.genEGNodeBName(null, "(WCDMA3G)", 50000, "NODECODEtEST"));
    }
    @Test
    void  genNodeBName(){
        assertEquals("NODECODEtEST3",quollUtils.genNodeBName("(3)", "NODECODEtEST"));
        assertEquals("NODECODEtESTZ",quollUtils.genNodeBName("(33)", "NODECODEtEST"));
        assertEquals("NODECODEtEST1",quollUtils.genNodeBName("TEST", "NODECODEtEST"));


    }
}