// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;

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
 * Test that SELECT statements with function calls in the expression list
 * operate like we expect them to.
 */
public class TestFunctions extends RtsqlTestCase {

  /**
   * Run a test where one records of two integer-typed fields is selected from
   * two input records.
   * @param query the query string to submit to the execution engine.
   * @param checkFields a list of (string, object) pairs naming an output field and
   * its value to verify.
   */
  private void runFnTest(String query, List<Pair<String, Object>> checkFields)
      throws IOException, InterruptedException {
    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getNullable(Type.TypeName.INT)));
    streamBuilder.addEvent("1,2");
    streamBuilder.addEvent("3,-4");
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
  public void testFn1() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("d", Integer.valueOf(4)));
    runFnTest("SELECT square_int(a) as c, square_int(b) as d FROM memstream WHERE a = 1",
        checks);
  }

  @Test
  public void testFn2() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    runFnTest("SELECT a, square_int(a) as c FROM memstream WHERE a = 1",
        checks);
  }

  @Test
  public void testFn3() throws IOException, InterruptedException {
    // Test that ints are coerced to strings in fn args.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    runFnTest("SELECT length(10 * a) as c FROM memstream WHERE a = 1",
        checks);
  }

  @Test
  public void testWhereFn() throws IOException, InterruptedException {
    // Test that function calls in the WHERE clause are ok.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(-4)));
    runFnTest("SELECT a, b FROM memstream WHERE length(b) = 2",
        checks);
  }

  @Test
  public void testFailNotFound() throws IOException, InterruptedException {
    // Test that a function that doesn't exist, fails.
    boolean failed = false;

    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
      runFnTest("SELECT meepmeepmeep(a) FROM memstream", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected: meepmeepmeep() is not a function. This should not typecheck.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }

  @Test
  public void testFailNotAFunction() throws IOException, InterruptedException {
    // Test that a symbol that isn't a function, fails.
    boolean failed = false;

    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
      runFnTest("SELECT b(a) FROM memstream", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected: b is a field not a function. This should not typecheck.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }

  @Test
  public void testFailTooFewArgs() throws IOException, InterruptedException {
    // Test that too few arguments causes failure.
    boolean failed = false;

    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
      runFnTest("SELECT length() FROM memstream", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected: b is a field not a function. This should not typecheck.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }

  @Test
  public void testFailTooManyArgs() throws IOException, InterruptedException {
    // Test that too many arguments causes failure.
    boolean failed = false;

    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
      runFnTest("SELECT length(a, b) FROM memstream", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected: b is a field not a function. This should not typecheck.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }

}
