package com.spark.transformations.util;

import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollSchemas;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class QuollUtilsTest {
    SparkSession session ;
    QuollUtils quollUtils;
    Dataset testDataset;
    Dataset tespest;
    @BeforeEach
    void setUp() {
        session = SparkSession.builder().appName("QuollTransformationsTest")
                .master("local[1]")
                .getOrCreate();
        quollUtils = new QuollUtils();
        testDataset = quollUtils.readFile(session, null, "src/test/resources/testoneinput.csv");
        tespest = quollUtils.readFile(session,  QuollSchemas.nodeIdSchema, "src/test/resources/in/TEMPEST.csv");
        System.out.println("=======================================");
        tespest.show();
tespest.where(tespest.col("network").isin("5G (NGRAN)").and(tespest.col("rbs_id").isNotNull())).show();
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

    }

    @Test
    void cleanRac() {
    }

    @Test
    void validateMscNode() {
    }

    @Test
    void cleanBool() {
    }

    @Test
    void cleanYN() {
    }

    @Test
    void cleanLac() {
    }

    @Test
    void cleanRacVal() {
    }

    @Test
    void cleanInt() {
    }

    @Test
    void mapAreaCode() {
    }

    @Test
    void cleanUra() {
    }

    @Test
    void classTag() {
    }

    @Test
    void isInteger() {
    }

    @Test
    void technologyToType() {
    }

    @Test
    void getBbhType() {
    }

    @Test
    void cleanTechnology() {
    }

    @Test
    void genEGNodeBName() {
    }

    @Test
    void genNodeBName() {
    }

    @Test
    void extractNameFromMecontext() {
    }

    @Test
    void extractIdFromMecontext() {
    }

    @Test
    void enmGnbType() {
    }
}