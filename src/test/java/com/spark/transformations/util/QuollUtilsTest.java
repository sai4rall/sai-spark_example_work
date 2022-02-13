package com.spark.transformations.util;

import com.spark.transformations.config.Constants;
import com.spark.transformations.config.QuollSchemas;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class QuollUtilsTest {
    SparkSession session ;
    QuollUtils quollUtils;
    Dataset testDataset;
    @BeforeEach
    public  void initialize(){
        session= SparkSession.builder().appName("QuollTransformationsTest")
                .master("local[1]")
                .getOrCreate();
        quollUtils=new QuollUtils();
        testDataset= quollUtils.readFile(session,null,"src/test/resources/testoneinput.csv");

    }
    @Test
    public void readFileTest(){
       Dataset q= quollUtils.readFile(session,null,"src/test/resources/in/TEMPEST.csv");
       assertEquals(3,q.count());
        Dataset t = quollUtils.readFile(session, QuollSchemas.nodeIdSchema, Constants.TEMPEST_NODE_ID_PATH);
        assertEquals(3,t.count());
    }
    @Test
public void applyInitialTrabsformaionsTest(){
        Dataset q=quollUtils.applyInitialTrabsformaions(testDataset);
    assertEquals(5,q.count());


}
@Test
public void cleanlyConvertssitesToInteger(){
    Dataset q=quollUtils.applyInitialTrabsformaions(testDataset);
    q = quollUtils.addAdditionalAttributes(q);
    Dataset sites=quollUtils.cleanlyConvertssitesToInteger(q);
    assertEquals(5,sites.count());
}

    @AfterEach
    void tearDown() {
        session.stop();
    }
}