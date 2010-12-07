// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericData;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.util.Pair;

import com.odiago.rtengine.exec.local.LocalEnvironment;
import com.odiago.rtengine.exec.local.MemoryOutputElement;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.testutil.MemStreamBuilder;
import com.odiago.rtengine.testutil.RtsqlTestCase;

import static org.junit.Assert.*;

/**
 * Test that SELECT statements operate like we expect them to.
 */
public class TestSelect extends RtsqlTestCase {

  @Test
  public void testSelectOneCol() throws IOException, InterruptedException {
    // Given three input records with a single column, select the column
    // and get the same result back.

    // Create the stream, and put some records in it.
    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("fieldname", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addEvent("1");
    streamBuilder.addEvent("2");
    streamBuilder.addEvent("3");
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery("SELECT fieldname FROM memstream");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer expected = Integer.valueOf(i + 1);
      GenericData.Record record = outRecords.get(i);
      assertEquals(expected, record.get("fieldname"));
    }
  }

  @Test
  public void testSelectNoRecords() throws IOException, InterruptedException {
    // Populate a stream with no records, make sure a SELECT statement on
    // this empty data set is okay.

    // Create the stream, and put some records in it.
    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("fieldname", Type.getPrimitive(Type.TypeName.INT)));
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery("SELECT fieldname FROM memstream");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);
    List<GenericData.Record> outRecords = output.getRecords();
    assertNotNull(outRecords);
    assertEquals(0, outRecords.size());
  }

  /**
   * Run a test where three records of two integer-typed fields are selected.
   * @param streamName the stream to create and populate with three records.
   * @param leftFieldName the name to assign to the first field.
   * @param rightFieldName the name to assign to the second field.
   * @param query the query string to submit to the execution engine.
   * @param checkLeft true if we should verify the receipt of the left field in the
   * output dataset.
   * @param checkRight true if we should verify the receipt of the right field in the
   * output dataset.
   */
  private void runTwoFieldTest(String streamName, String leftFieldName, String rightFieldName,
      String query, boolean checkLeft, boolean checkRight)
      throws IOException, InterruptedException {
    MemStreamBuilder streamBuilder = new MemStreamBuilder(streamName);

    streamBuilder.addField(new TypedField(leftFieldName, Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField(rightFieldName, Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addEvent("1,-1");
    streamBuilder.addEvent("2,-2");
    streamBuilder.addEvent("3,-3");
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query);
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer expected = Integer.valueOf(i + 1);
      Integer negative = Integer.valueOf(-i - 1);
      GenericData.Record record = outRecords.get(i);
      if (checkLeft) {
        assertEquals(expected, record.get(leftFieldName));
      }

      if (checkRight) {
        assertEquals(negative, record.get(rightFieldName));
      }
    }
  }

  @Test
  public void testSelectTwoFields() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b'; select both of them.
    runTwoFieldTest("memstream", "a", "b", "SELECT a, b FROM memstream", true, true);
  }

  @Test
  public void testProjection() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b'; select only the "b" field.
    runTwoFieldTest("memstream", "a", "b", "SELECT b FROM memstream", false, true);
  }

  @Test
  public void testRearrange() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b', select 'b, a'.
    // For good measure, put this entire query in UPPER CASE to ensure
    // we canonicalize case for stream, field names.
    runTwoFieldTest("memstream", "a", "b", "SELECT B, A FROM MEMSTREAM", true, true);
  }

  @Test
  public void testCaseSensitiveStream1() throws IOException, InterruptedException {
    // Verify that we are capable of reading a case sensitive stream by
    // quoting the stream name.
    runTwoFieldTest("CaseSensitiveStream", "a", "b", "SELECT a, b FROM \"CaseSensitiveStream\"",
        true, true);
  }

  @Test
  public void testCaseSensitiveStream2() throws IOException, InterruptedException {
    // And that if we don't quote the stream name, this fails because of
    // canonicalization within the user's statement.
    boolean failed = false;
    try {
      runTwoFieldTest("CaseSensitiveStream", "a", "b", "SELECT a, b FROM CaseSensitiveStream",
          true, true);
      failed = true;
    } catch (AssertionError ae) {
      // We expect this one to fail internally, and get here. This is ok.
    }

    if (failed) {
      fail("Expected internal test to fail, but it didn't");
    }
  }

  @Test
  public void testCaseSensitiveFields1() throws IOException, InterruptedException {
    // Verify that we are capable of reading a case sensitive field name by quoting it. 
    runTwoFieldTest("memstream", "Aa", "b", "SELECT \"Aa\", b FROM memstream",
        true, true);
  }

  @Test
  public void testCaseSensitiveFields2() throws IOException, InterruptedException {
    // And that if we don't quote the stream name, this fails because of
    // canonicalization within the user's statement.
    boolean failed = false;
    try {
      runTwoFieldTest("memstream", "Aa", "b", "SELECT Aa, b FROM memstream",
          true, true);
      failed = true;
    } catch (AssertionError ae) {
      // We expect this one to fail internally, and get here. This is ok.
    }

    if (failed) {
      fail("Expected internal test to fail, but it didn't");
    }
  }

  @Test
  public void testSelectFieldTwice() throws IOException, InterruptedException {
    // Test that all the correct data comes back if we select the same field twice.
    runTwoFieldTest("memstream", "a", "b", "SELECT a, a, b, a FROM memstream", true, true);
  }

  @Test
  public void testKeywordStreamName1() throws IOException, InterruptedException {
    // Test that it's ok to use a keyword as a stream name, if you quote it.
    runTwoFieldTest("select", "a", "b", "SELECT a, b FROM \"select\"", true, true);
  }

  @Test
  public void testKeywordStreamName2() throws IOException, InterruptedException {
    // Test that it's ok to use a keyword as a stream name, if you quote it.
    runTwoFieldTest("SELECT", "a", "b", "SELECT a, b FROM \"SELECT\"", true, true);
  }

  @Test
  public void testKeywordStreamName3() throws IOException, InterruptedException {
    // Check that this fails, like we expect it to, without quotes.
    boolean failed = false;
    try {
      runTwoFieldTest("select", "a", "b", "SELECT a, b FROM select", true, true);
      failed = true;
    } catch (AssertionError ae) {
      // Expected; ok.
    }

    if (failed) {
      fail("Expected internal test to fail, but it didn't.");
    }
  }

  @Test
  public void testKeywordFieldName() throws IOException, InterruptedException {
    // Test that it's ok to use a keyword as a field name, if you quote it.
    runTwoFieldTest("memstream", "SELECT", "b", "SELECT \"SELECT\", b FROM memstream",
        true, true);
  }

  @Test
  public void testSelectStar() throws IOException, InterruptedException {
    // Test that it's ok to use a keyword as a field name, if you quote it.
    runTwoFieldTest("memstream", "a", "b", "SELECT * FROM memstream",
        true, true);
  }

  /**
   * Run a test where three records of two integer-typed fields are selected.
   * @param streamName the stream to create and populate with one record.
   * @param leftFieldName the name to assign to the first field.
   * @param rightFieldName the name to assign to the second field.
   * @param query the query string to submit to the execution engine.
   * @param checkFields a list of (string, object) pairs naming an output field and
   * its value to verify.
   */
  private void runFreeSelectTest(String streamName, String leftFieldName, String rightFieldName,
      String query, List<Pair<String, Object>> checkFields)
      throws IOException, InterruptedException {
    MemStreamBuilder streamBuilder = new MemStreamBuilder(streamName);

    streamBuilder.addField(new TypedField(leftFieldName, Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField(rightFieldName, Type.getNullable(Type.TypeName.INT)));
    streamBuilder.addEvent("1,2");
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query);
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    GenericData.Record firstRecord = outRecords.get(0);
    for (Pair<String, Object> check : checkFields) {
      String checkName = check.getLeft();
      Object checkVal = check.getRight();
      assertEquals(checkVal, firstRecord.get(checkName));
    }
  }

  @Test
  public void testAddExpr() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT a, b, 3 + a AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Integer.valueOf(4))));
  }

  @Test
  public void testAddExpr2() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT a, b, a + 3 AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Integer.valueOf(4))));
  }

  @Test
  public void testAddExpr3() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT a + b AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Integer.valueOf(3))));
  }

  @Test
  public void testSub1() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT b - 1 AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Integer.valueOf(1))));
  }

  @Test
  public void testSub2() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT 1 - b AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Integer.valueOf(-1))));
  }

  @Test
  public void testBoolean() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT true AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Boolean.TRUE)));
  }
 
  @Test
  public void testAnd() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT true AND true AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Boolean.TRUE)));
  }
 
  @Test
  public void testAnd2() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT false AND true AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Boolean.FALSE)));
  }
 
  @Test
  public void testOr() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    runFreeSelectTest("memstream", "a", "b", "SELECT false OR true AS c FROM memstream",
        Collections.singletonList(new Pair<String, Object>("c", Boolean.TRUE)));
  }
 
  @Test
  public void testMultiExpr() throws IOException, InterruptedException {
    // Test that we can add a value to the first field and select it.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("d", Integer.valueOf(2)));
    runFreeSelectTest("memstream", "a", "b", "SELECT 1 AS c, 2 as d FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasAToB() throws IOException, InterruptedException {
    // Test that we can select 'a' as 'b' when b would otherwise exist.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));
    runFreeSelectTest("memstream", "a", "b", "SELECT a AS b FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasBoth() throws IOException, InterruptedException {
    // Test that we can select 'a' as 'b' and b to a.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("a", Integer.valueOf(2)));
    runFreeSelectTest("memstream", "a", "b", "SELECT a AS b, b AS a FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasBoth2() throws IOException, InterruptedException {
    // Test that order doesn't matter.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("a", Integer.valueOf(2)));
    runFreeSelectTest("memstream", "a", "b", "SELECT b AS a, a AS b FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasBoth3() throws IOException, InterruptedException {
    // Test that we can select 'a' as 'b' and b to c.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    runFreeSelectTest("memstream", "a", "b", "SELECT a AS b, b AS c FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasAndExpr() throws IOException, InterruptedException {
    // Test that expressions run with the unprojected column names.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    runFreeSelectTest("memstream", "a", "b", "SELECT a AS b, a + 1 AS c FROM memstream",
        checks);
  }
 
  @Test
  public void testAliasAndExpr2() throws IOException, InterruptedException {
    // Test that expressions run with the unprojected column names.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));

    boolean failed = false;
    try {
      runFreeSelectTest("memstream", "a", "b", "SELECT a AS x, x + 1 AS c FROM memstream",
          checks);
      failed = true;
    } catch (AssertionError ae) {
      // expected.
    }

    if (failed) {
      fail("Failed; this should have had an error, since x is not a real field.");
    }
  }

  @Test
  public void testOverlaps() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(3)));

    runFreeSelectTest("memstream", "a", "b", "SELECT a, b AS c, 1 + 2 as b FROM memstream",
        checks);
  }

  @Test
  public void testNestedSelect1() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT * FROM (SELECT a, b FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect2() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT b, a FROM (SELECT a, b FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect3() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(2)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(1)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT b, a FROM (SELECT a as b, b as a FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect4() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("d", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT c, d FROM (SELECT a as c, b as d FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect5() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("d", Integer.valueOf(4)));
    checks.add(new Pair<String, Object>("e", Integer.valueOf(42)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT c, d, e FROM (SELECT a as c, 2 * b as d, 42 e FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect6() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT B, A FROM (SELECT a, b FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect7() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT B, A FROM (SELECT * FROM memstream)",
        checks);
  }

  @Test
  public void testNestedSelect8() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));

    runFreeSelectTest("memstream", "a", "b",
        "SELECT * FROM (SELECT * FROM memstream)",
        checks);
  }

  // TODO: Write the following tests:
  //   Test string fields.
  //   Test long integer fields.
  //   Test boolean fields.
  //   Test nullable int fields.
  //   Test nullable string fields.
  //
  // TestDropStream:
  //   Test that a DROP STREAM followed by a SELECT fails on that stream.
  //   Test that a DROP STREAM followed by a SELECT on a different stream is ok.
  //
  //
}
