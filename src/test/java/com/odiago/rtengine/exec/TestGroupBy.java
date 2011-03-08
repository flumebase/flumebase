// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericData;

import org.testng.annotations.Test;

import com.odiago.rtengine.exec.local.LocalEnvironment;
import com.odiago.rtengine.exec.local.MemoryOutputElement;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.testutil.MemStreamBuilder;
import com.odiago.rtengine.testutil.RtsqlTestCase;

import static org.testng.AssertJUnit.*;

/**
 * Test that SELECT statements with GROUP BY and OVER clauses operate like we expect them to.
 */
public class TestGroupBy extends RtsqlTestCase {

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
  private List<GenericData.Record> submitQuery(StreamSymbol stream,
      String query) throws IOException, InterruptedException {

    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testGroupBy");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query, getQueryOpts());
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testGroupBy");
    assertNotNull(output);

    return output.getRecords();
  }

  @Test
  public void testBasicCount() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Basic test.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have two output results: 2 at t=40, 3 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(2));
      assertRecordExists(results, "c", Integer.valueOf(3));
    }
  }

  @Test
  public void testCountOfNullField() throws IOException, InterruptedException {
    // Put a null field in b, it should not get counted.
    String [] records = { "0,10", "1,", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have two output results: 1 at t=40, 2 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(1));
      assertRecordExists(results, "c", Integer.valueOf(2));
    }
  }

  @Test
  public void testNamedWindow() throws IOException, InterruptedException {
    // Same as testCountOfNullField(), but use a named WINDOW clause.
    String [] records = { "0,10", "1,", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER win WINDOW win AS (RANGE INTERVAL 1 SECONDS PRECEDING)");

    // We should have two output results: 1 at t=40, 2 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(1));
      assertRecordExists(results, "c", Integer.valueOf(2));
    }
  }

  @Test
  public void testCountRecordsForNullField() throws IOException, InterruptedException {
    // Put in a record entirely of null values; COUNT(1) should still count all the records.
    String [] records = { "0,10", ",", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(1) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have two output results: 2 at t=40, 3 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(2));
      assertRecordExists(results, "c", Integer.valueOf(3));
    }
  }

  @Test
  public void testCountStar() throws IOException, InterruptedException {
    // Put in a record entirely of null values; COUNT(*) should count this record too.
    String [] records = { "0,10", ",", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(*) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have two output results: 2 at t=40, 3 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(2));
      assertRecordExists(results, "c", Integer.valueOf(3));
    }
  }

  @Test
  public void testSum() throws IOException, InterruptedException {
    // Test that the SUM function works like we expect it to.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 35, 36, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT SUM(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have two output results: 21 at t=40, 33 at t=200.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(2, results.size());
      assertRecordExists(results, "c", Integer.valueOf(21));
      assertRecordExists(results, "c", Integer.valueOf(33));
    }
  }

  @Test
  public void testEviction() throws IOException, InterruptedException {
    // Test that older values do roll off the end...
    String [] records = { "0,10", "1,11", "2,12", "3,13" };
    long [] times = { 35, 36, 200, 1150 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT SUM(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should get the following results: 21, 33, 25
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(results, "c", Integer.valueOf(21));
      assertRecordExists(results, "c", Integer.valueOf(33));
      assertRecordExists(results, "c", Integer.valueOf(25));
    }
  }

  @Test
  public void testMax() throws IOException, InterruptedException {
    // Test the MAX function.
    String [] records = { "0,10", "1,11", "2,12", "3,13" };
    long [] times = { 35, 36, 200, 1150 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT MAX(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should get the following results: 11, 12, 13
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(results, "c", Integer.valueOf(11));
      assertRecordExists(results, "c", Integer.valueOf(12));
      assertRecordExists(results, "c", Integer.valueOf(13));
    }
  }

  @Test
  public void testMin() throws IOException, InterruptedException {
    // Test the MIN function.
    String [] records = { "0,10", "1,11", "2,12", "3,13" };
    long [] times = { 35, 36, 200, 1150 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT MIN(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should get the following results: 10, 10, 12
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(results, "c", Integer.valueOf(10));
      assertRecordExists(results, "c", Integer.valueOf(10));
      assertRecordExists(results, "c", Integer.valueOf(12));
    }
  }

  @Test
  public void testGrouping() throws IOException, InterruptedException {
    // Test that SUM works correctly in the presence of a GROUP BY clause.
    String [] records = { "0,10", "1,11", "1,9", "0,12" };
    long [] times = { 35, 36, 37, 200 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT a, SUM(b) AS c FROM s GROUP BY a OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have the following results:
    // (0, 10)
    // (1, 20)
    // (0, 22)
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());

      assertRecordFields(Collections.singletonList(results.get(0)),
          "a", Integer.valueOf(0), "c", Integer.valueOf(10));
      assertRecordFields(Collections.singletonList(results.get(1)),
          "a", Integer.valueOf(1), "c", Integer.valueOf(20));
      assertRecordFields(Collections.singletonList(results.get(2)),
          "a", Integer.valueOf(0), "c", Integer.valueOf(22));
    }
  }

  @Test
  public void testBoundaries1() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just after window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 1000, 2000 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results; all have value '1'.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(1));
    }
  }

  @Test
  public void testBoundaries2() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 999, 2000 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 1.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(1));
    }
  }

  @Test
  public void testBoundaries3() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 990, 2000 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 1.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(1));
    }
  }

  @Test
  public void testBoundaries4() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 990, 1990 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 1.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(1));
    }
  }

  @Test
  public void testBoundaries5() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 990, 1989 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 2.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(2));
    }
  }

  @Test
  public void testBoundaries6() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 500, 1500 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 1.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(1));
    }
  }

  @Test
  public void testBoundaries7() throws IOException, InterruptedException {
    // COUNT() the values of the 'b' column. Test that this works correctly
    // when values are arriving just ahead of window boundaries.
    String [] records = { "0,10", "1,11", "2,12" };
    long [] times = { 0, 500, 1499 };

    StreamSymbol stream = makeStream("s", "a", "b", records, times);

    List<GenericData.Record> results = submitQuery(stream,
        "SELECT COUNT(b) AS c FROM s OVER RANGE INTERVAL 1 SECONDS PRECEDING");

    // We should have three output results: 1, 2, 2.
    assertNotNull(results);
    synchronized (results) {
      assertEquals(3, results.size());
      assertRecordExists(Collections.singletonList(results.get(0)), "c", Integer.valueOf(1));
      assertRecordExists(Collections.singletonList(results.get(1)), "c", Integer.valueOf(2));
      assertRecordExists(Collections.singletonList(results.get(2)), "c", Integer.valueOf(2));
    }
  }
}
