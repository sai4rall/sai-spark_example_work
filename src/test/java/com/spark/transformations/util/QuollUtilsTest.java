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
    @BeforeEach
    void setUp() {
        session = SparkSession.builder().appName("QuollTransformationsTest")
                .master("local[1]")
                .getOrCreate();
        quollUtils = new QuollUtils();
        testDataset = quollUtils.readFile(session, null, "src/test/resources/testoneinput.csv");

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
        assertEquals(5, q.count());

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
        assertEquals(4, bscdata.count());
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
        assertEquals(4, btsLookup.count());
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