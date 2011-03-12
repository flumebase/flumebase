// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.annotations.Test;

import com.cloudera.util.Pair;

import com.odiago.flumebase.exec.local.LocalEnvironment;
import com.odiago.flumebase.exec.local.MemoryOutputElement;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.testutil.MemStreamBuilder;
import com.odiago.flumebase.testutil.RtsqlTestCase;

import static org.testng.AssertJUnit.*;

/**
 * Test the throughput of the event processing engine. "Correctness"
 * in the pass/fail sense of this test is based on verifying the
 * correct number of output records appear, but this is not the main
 * point of the exercise; the test output text will contain measurements
 * of the performance of the system which should be noted by test
 * engineers.
 */
public class TestThroughput extends RtsqlTestCase {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestThroughput.class.getName());

  private static final String STREAM_NAME = "throughputstream";

  private static final int NUM_RECORDS = 50000; // number of records per test.

  /**
   * Run a test where 50,000 records are pre-materialized and fed
   * into the execution engine as quickly as possible. The time
   * required to perform the test is noted on the output. The size of
   * each record can be controlled by the size of the string object
   * propagated through it.
   * @param query the query string to submit to the execution engine.
   * @param checkFields a list of (string, object) pairs naming an output field and
   * its value to verify.
   */
  private void runThroughputTest(String query, int stringSize, int expectedRecordCount)
      throws IOException, InterruptedException {

    long initTimestamp = System.currentTimeMillis();

    MemStreamBuilder streamBuilder = new MemStreamBuilder(STREAM_NAME);
    long bytesSent = 0;

    StringBuilder sbStringCol = new StringBuilder();
    for (int i = 0; i < stringSize; i++) {
      sbStringCol.append("a");
    }

    String stringCol = sbStringCol.toString();

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getNullable(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("c", Type.getNullable(Type.TypeName.STRING)));
    for (int i = 0; i < NUM_RECORDS; i++) {
      StringBuilder sb = new StringBuilder();
      sb.append(i);
      sb.append(",");
      int j = 10 * i;
      sb.append(j);
      sb.append(",");
      sb.append(stringCol);
      String eventText = sb.toString();
      streamBuilder.addEvent(eventText);
      bytesSent += eventText.length();
    }
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testThroughput");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    long startTimestamp = System.currentTimeMillis();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query, getQueryOpts());
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    joinFlow(id);

    long stopTimestamp = System.currentTimeMillis();

    // Examine the response records.
    MemoryOutputElement output = getOutput("testThroughput");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    assertNotNull(outRecords);
    assertEquals("Improper number of records!", expectedRecordCount, outRecords.size());

    long initTime = startTimestamp - initTimestamp;
    long runTime = stopTimestamp - startTimestamp;
    long eventsPerSecond = (NUM_RECORDS * 1000) / runTime;
    long bytesPerSecond = (bytesSent * 1000) / runTime;
    LOG.info("Query: " + query);
    LOG.info("Init time: " + initTime);
    LOG.info("Run time: " + runTime);
    LOG.info("Num events: " + NUM_RECORDS);
    LOG.info("Num input bytes: " + bytesSent);
    LOG.info("Events per second: " + eventsPerSecond);
    LOG.info("Bytes per second: " + bytesPerSecond);
  }

  @Test(groups = { "slow" })
  public void testAll() throws IOException, InterruptedException {
    runThroughputTest("SELECT * FROM " + STREAM_NAME, 32, NUM_RECORDS);
  }

  @Test(groups = { "slow" })
  public void testMod() throws IOException, InterruptedException {
    runThroughputTest("SELECT * FROM " + STREAM_NAME + " WHERE a % 10 = 0", 32, NUM_RECORDS / 10);
  }

  @Test(groups = { "slow" })
  public void testLargeRecord() throws IOException, InterruptedException {
    runThroughputTest("SELECT * FROM " + STREAM_NAME + " WHERE a % 10 = 0", 1024,
        NUM_RECORDS / 10);
  }
}
