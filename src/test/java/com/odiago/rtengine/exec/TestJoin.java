// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.List;

import org.apache.avro.generic.GenericData;

import org.junit.Test;

import com.odiago.rtengine.exec.local.LocalEnvironment;
import com.odiago.rtengine.exec.local.MemoryOutputElement;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.testutil.MemStreamBuilder;
import com.odiago.rtengine.testutil.RtsqlTestCase;

import static org.junit.Assert.*;

/**
 * Test that SELECT statements with JOIN clauses operate like we expect them to.
 */
public class TestJoin extends RtsqlTestCase {

  /**
   * Create a stream with two columns which have configurable names. The first column
   * is of type "INT NOT NULL," the second column is of type "INT".
   */
  private StreamSymbol makeStream(String streamName, String leftColName,
      String rightColName, String [] eventTexts, long [] eventTimestamps) {
    MemStreamBuilder streamBuilder = new MemStreamBuilder(streamName);
    streamBuilder.addField(new TypedField(leftColName, Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField(rightColName, Type.getNullable(Type.TypeName.INT)));

    for (int i = 0; i < eventTexts.length; i++) {
      streamBuilder.addEvent(eventTexts[i], eventTimestamps[i]);
    }

    return streamBuilder.build();
  }

  /**
   * Run the test, where we submit the query to the processing engine.
   * @return The set of output record from the query.
   */
  private List<GenericData.Record> submitQuery(StreamSymbol leftStream,
      StreamSymbol rightStream, String query) throws IOException, InterruptedException {

    getSymbolTable().addSymbol(leftStream);
    getSymbolTable().addSymbol(rightStream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testJoin");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query, getQueryOpts());
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testJoin");
    assertNotNull(output);

    return output.getRecords();
  }

  @Test
  public void testBasicJoin() throws IOException, InterruptedException {
    String [] leftRecords = { "0,10", "1,11", "2,12" };
    long [] leftTimes = { 0, 1, 2 };
    String [] rightRecords = { "0,20", "1,21", "2,22" };
    long [] rightTimes = { 0, 1, 2 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON a=c OVER RANGE INTERVAL 1 MINUTES PRECEDING");

    // We should have three output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());

      // Assert that b == d for all records.
      assertRecordFields(results, "a", Integer.valueOf(0), "c", Integer.valueOf(0));
      assertRecordFields(results, "a", Integer.valueOf(1), "c", Integer.valueOf(1));
      assertRecordFields(results, "a", Integer.valueOf(2), "c", Integer.valueOf(2));

      // Assert the join fields are present.
      assertRecordFields(results, "a", Integer.valueOf(0), "d", Integer.valueOf(20));
      assertRecordFields(results, "a", Integer.valueOf(1), "d", Integer.valueOf(21));
      assertRecordFields(results, "a", Integer.valueOf(2), "d", Integer.valueOf(22));
    }
  }

  @Test
  public void testNullableFieldJoin1() throws IOException, InterruptedException {
    // Run the basic test but use a NULLABLE INT field.
    String [] leftRecords = { "10,0", "11,1", "12,2" };
    long [] leftTimes = { 0, 1, 2 };
    String [] rightRecords = { "20,0", "21,1", "22,2" };
    long [] rightTimes = { 0, 1, 2 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON b=d OVER RANGE INTERVAL 1 MINUTES PRECEDING");

    // We should have three output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());

      // Assert that a == c for all records.
      assertRecordFields(results, "b", Integer.valueOf(0), "d", Integer.valueOf(0));
      assertRecordFields(results, "b", Integer.valueOf(1), "d", Integer.valueOf(1));
      assertRecordFields(results, "b", Integer.valueOf(2), "d", Integer.valueOf(2));

      // Assert the join fields are present.
      assertRecordFields(results, "b", Integer.valueOf(0), "c", Integer.valueOf(20));
      assertRecordFields(results, "b", Integer.valueOf(1), "c", Integer.valueOf(21));
      assertRecordFields(results, "b", Integer.valueOf(2), "c", Integer.valueOf(22));
    }
  }

  @Test
  public void testNullableFieldJoin2() throws IOException, InterruptedException {
    // Test that an inner join gracefully ignores records where one side is null.
    String [] leftRecords = { "10,0", "11,", "12,2" };
    long [] leftTimes = { 0, 1, 2 };
    String [] rightRecords = { "20,0", "21,1", "22,2" };
    long [] rightTimes = { 0, 1, 2 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON b=d OVER RANGE INTERVAL 1 MINUTES PRECEDING");

    // We should have two output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());

      // Assert that b == d for all records.
      assertRecordFields(results, "b", Integer.valueOf(0), "d", Integer.valueOf(0));
      assertRecordFields(results, "b", Integer.valueOf(2), "d", Integer.valueOf(2));

      // Assert the join fields are present.
      assertRecordFields(results, "b", Integer.valueOf(0), "c", Integer.valueOf(20));
      assertRecordFields(results, "b", Integer.valueOf(2), "c", Integer.valueOf(22));
    }
  }

  @Test
  public void testNullableFieldJoin3() throws IOException, InterruptedException {
    // Test that an inner join gracefully ignores records where the other side is null.
    String [] leftRecords = { "10,0", "11,1", "12,2" };
    long [] leftTimes = { 0, 1, 2 };
    String [] rightRecords = { "20,0", "21,", "22,2" };
    long [] rightTimes = { 0, 1, 2 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON b=d OVER RANGE INTERVAL 1 MINUTES PRECEDING");

    // We should have two output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());

      // Assert that b == d for all records.
      assertRecordFields(results, "b", Integer.valueOf(0), "d", Integer.valueOf(0));
      assertRecordFields(results, "b", Integer.valueOf(2), "d", Integer.valueOf(2));

      // Assert the join fields are present.
      assertRecordFields(results, "b", Integer.valueOf(0), "c", Integer.valueOf(20));
      assertRecordFields(results, "b", Integer.valueOf(2), "c", Integer.valueOf(22));
    }
  }

  @Test
  public void testIgnoreOlderRight() throws IOException, InterruptedException {
    // Test that a right-side record that is too old is not used in the join.
    String [] leftRecords = { "0,10", "1,11", "2,12" };
    long [] leftTimes = { 5000, 5001, 5002 };
    String [] rightRecords = { "0,20", "1,21", "2,22" };
    long [] rightTimes = { 1000, 4999, 5000 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON a=c OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());

      assertRecordFields(results, "a", Integer.valueOf(1), "c", Integer.valueOf(1));
      assertRecordFields(results, "a", Integer.valueOf(2), "c", Integer.valueOf(2));

      // Assert the join fields are present.
      assertRecordFields(results, "a", Integer.valueOf(1), "d", Integer.valueOf(21));
      assertRecordFields(results, "a", Integer.valueOf(2), "d", Integer.valueOf(22));
    }
  }

  @Test
  public void testIgnoreNewerLeft() throws IOException, InterruptedException {
    // Test that a left-side record that is too new is not used in the join.
    String [] leftRecords = { "0,10", "1,11", "2,12" };
    long [] leftTimes = { 5000, 5001, 15002 };
    String [] rightRecords = { "0,20", "1,21", "2,22" };
    long [] rightTimes = { 4901, 4999, 5000 };

    StreamSymbol leftStream = makeStream("lt", "a", "b", leftRecords, leftTimes);
    StreamSymbol rightStream = makeStream("rt", "c", "d", rightRecords, rightTimes);

    List<GenericData.Record> results = submitQuery(leftStream, rightStream,
        "SELECT * FROM lt JOIN rt ON a=c OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());

      assertRecordFields(results, "a", Integer.valueOf(2), "c", Integer.valueOf(0));
      assertRecordFields(results, "a", Integer.valueOf(1), "c", Integer.valueOf(1));

      // Assert the join fields are present.
      assertRecordFields(results, "a", Integer.valueOf(0), "d", Integer.valueOf(20));
      assertRecordFields(results, "a", Integer.valueOf(1), "d", Integer.valueOf(21));
    }
  }
}
