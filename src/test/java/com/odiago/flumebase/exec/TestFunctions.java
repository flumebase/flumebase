// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;

import org.apache.avro.util.Utf8;

import org.testng.annotations.Test;

import com.cloudera.util.Pair;

import com.odiago.flumebase.exec.local.LocalEnvironment;
import com.odiago.flumebase.exec.local.MemoryOutputElement;

import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.testutil.MemStreamBuilder;
import com.odiago.flumebase.testutil.RtsqlTestCase;

import static org.testng.AssertJUnit.*;

/**
 * Test that SELECT statements with function calls in the expression list
 * operate like we expect them to.
 */
public class TestFunctions extends RtsqlTestCase {

  /**
   * Scalar function that returns the larger of two values.
   */
  private static class max2 extends ScalarFunc {
    private UniversalType mArgType;

    public max2() {
      mArgType = new UniversalType("'a");
    }

    @Override
    public Object eval(Object... args) {
      Object left = args[0];
      Object right = args[1];

      if (null == left) {
        return right;
      } else if (null == right) {
        return left;
      }

      int comp = ((Comparable) left).compareTo(right);
      if (comp >= 0) {
        return left;
      } else {
        return right;
      }
    }

    @Override
    public Type getReturnType() {
      // Our return type matches the types of both arguments.
      return mArgType;
    }

    @Override
    public List<Type> getArgumentTypes() {
      // Two arguments of the same type, but that type is unconstrained.
      List<Type> args = new ArrayList<Type>();
      args.add(mArgType);
      args.add(mArgType);
      return args;
    }
  }

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

    // Register the 'max2' function we use in some tests.
    ScalarFunc max2Func = new max2();
    getSymbolTable().addSymbol(new FnSymbol("max2", max2Func, max2Func.getReturnType(),
        max2Func.getArgumentTypes()));

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    QuerySubmitResponse response = env.submitQuery(query, getQueryOpts());
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    joinFlow(id);

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
    runFnTest("SELECT square(a) as c, square(b) as d FROM memstream WHERE a = 1",
        checks);
  }

  @Test
  public void testFn2() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    runFnTest("SELECT a, square(a) as c FROM memstream WHERE a = 1",
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
      runFnTest("SELECT length(a, b) FROM memstream", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected: b is a field not a function. This should not typecheck.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }
  @Test
  public void testDoubleUnification0() throws IOException, InterruptedException {
    // Test that we can unify two instances of the exact same type.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(1)));
    runFnTest("SELECT max2(a, a) as c FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testDoubleUnification1() throws IOException, InterruptedException {
    // Test that we can unify two instances of the same type, but with one
    // of them nullable, and one of them not-null.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    runFnTest("SELECT max2(a, b) as c FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testDoubleUnification1a() throws IOException, InterruptedException {
    // Test that we can unify two instances of the same type.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", Integer.valueOf(2)));
    runFnTest("SELECT max2(b, a) as c FROM memstream WHERE a = 1", checks);
  }
  @Test
  public void testDoubleUnification2() throws IOException, InterruptedException {
    // Test that we can unify two instances of different types when a promotion
    // is possible.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", new Utf8("meep")));
    runFnTest("SELECT max2(a, 'meep') as c FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testDoubleUnification3() throws IOException, InterruptedException {
    // Like test #2, but with the argument order reversed.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", new Utf8("meep")));
    runFnTest("SELECT max2('meep', a) as c FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testDoubleUnification3a() throws IOException, InterruptedException {
    // Like test #3, but return the other value, demonstrating that
    // we definitely coerce from INT to STRING.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("c", new Utf8("1")));
    runFnTest("SELECT max2('', a) as c FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testDoubleUnification4() throws IOException, InterruptedException {
    // Verify that if there's a conflict in the argument types, we fail.
    boolean failed = false;
    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      runFnTest("SELECT max2(true, a) as c FROM memstream WHERE a = 1", checks);
      failed = true;
    } catch (AssertionError ae) {
      // We expected a failure in the test due to the type checker. Good.
    }

    if (failed) {
      fail("Expected type checker error, but the test succeeded!");
    }
  }

}
