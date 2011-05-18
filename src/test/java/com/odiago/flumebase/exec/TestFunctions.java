/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericData;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(
      TestFunctions.class.getName());

  /**
   * Scalar function that returns the larger of two values.
   */
  private static class max2 extends ScalarFunc {
    private UniversalType mArgType;

    public max2() {
      mArgType = new UniversalType("'a");
    }

    @Override
    public Object eval(EventWrapper event, Object... args) {
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
   * Var-arg function that concatenates a bunch of strings.
   */
  private static class concatstrs extends ScalarFunc {

    public concatstrs() {
    }

    @Override
    public Object eval(EventWrapper event, Object... args) {
      StringBuilder sb = new StringBuilder();

      for (Object arg : args) {
        if (null == arg) {
          sb.append("null");
        } else {
          assert arg instanceof CharSequence;
          sb.append(arg.toString());
        }
      }

      return new Utf8(sb.toString());
    }

    @Override
    public Type getReturnType() {
      return Type.getPrimitive(Type.TypeName.STRING);
    }

    @Override
    public List<Type> getArgumentTypes() {
      // No required args.
      return Collections.emptyList();
    }

    @Override
    public List<Type> getVarArgTypes() {
     return Collections.singletonList(Type.getNullable(Type.TypeName.STRING));
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
    LOG.info("Running function test: " + query);
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
        max2Func.getArgumentTypes(), max2Func.getVarArgTypes()));

    // Register the 'concatstrs' function we use in some tests.
    ScalarFunc strcatFunc = new concatstrs();
    getSymbolTable().addSymbol(new FnSymbol("concatstrs", strcatFunc, strcatFunc.getReturnType(),
        strcatFunc.getArgumentTypes(), strcatFunc.getVarArgTypes()));

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    // With all configuration complete, connect to the environment.
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Run the query.
    LOG.debug("Actually submitting to running environment");
    QuerySubmitResponse response = env.submitQuery(query, getQueryOpts());
    FlowId id = response.getFlowId();
    assertNotNull(response.getMessage(), id);
    joinFlow(id);
    LOG.debug("Flow runtime complete");

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

  @Test
  public void testVarArgs1() throws Exception {
    // Test that we can put a single value into a vararg fn.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", new Utf8("foo")));
    runFnTest("SELECT concatstrs('foo') AS x FROM memstream", checks);
  }

  @Test
  public void testVarArgsMulti() throws Exception {
    // Test that we can put a few values into a vararg fn.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", new Utf8("foobarbaz")));
    runFnTest("SELECT concatstrs('foo', 'bar', 'baz') AS x FROM memstream", checks);
  }

  @Test
  public void testVarArgsEmpty() throws Exception {
    // Test that we can put no values into a vararg fn.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", new Utf8("")));
    runFnTest("SELECT concatstrs() AS x FROM memstream", checks);
  }

  @Test
  public void testVarArgsCoerce() throws Exception {
    // Test that we can put values of different types into a vararg fn,
    // and they are all coerced to the correct type. 
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", new Utf8("foo42")));
    runFnTest("SELECT concatstrs('foo', 42) AS x FROM memstream", checks);
  }

  @Test
  public void testListFn() throws Exception {
    // Test that we can create a list.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    List<Object> innerList = new ArrayList<Object>();
    innerList.add(Integer.valueOf(1));
    innerList.add(Integer.valueOf(2));
    innerList.add(Integer.valueOf(3));
    checks.add(new Pair<String, Object>("x", innerList));
    runFnTest("SELECT to_list(1,2,3) AS x FROM memstream", checks);
  }

  @Test
  public void testEmptyList() throws Exception {
    // Test that we can create an empty list.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    List<Object> innerList = new ArrayList<Object>();
    checks.add(new Pair<String, Object>("x", innerList));
    runFnTest("SELECT to_list() AS x FROM memstream", checks);
  }

  @Test
  public void testNullListElem1() throws Exception {
    // Test that we can create a list with a null in it.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    List<Object> innerList = new ArrayList<Object>();
    innerList.add(null);
    checks.add(new Pair<String, Object>("x", innerList));
    runFnTest("SELECT to_list(null) AS x FROM memstream", checks);
  }

  @Test
  public void testNullListElem2() throws Exception {
    // Test that we can create a list with a null in it.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    List<Object> innerList = new ArrayList<Object>();
    innerList.add(Integer.valueOf(1));
    innerList.add(null);
    checks.add(new Pair<String, Object>("x", innerList));
    runFnTest("SELECT to_list(1, null) AS x FROM memstream", checks);
  }

  @Test
  public void testNullListElem3() throws Exception {
    // Test that we can create a list with a null in it.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    List<Object> innerList = new ArrayList<Object>();
    innerList.add(null);
    innerList.add(null);
    innerList.add(Integer.valueOf(1));
    checks.add(new Pair<String, Object>("x", innerList));
    runFnTest("SELECT to_list(null, null, 1) AS x FROM memstream", checks);
  }

  @Test
  public void testIndexFn1() throws Exception {
    // Test that we can index into a list.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Integer.valueOf(42)));
    runFnTest("SELECT index(to_list(1,42), 1) AS x FROM memstream", checks);
  }

  @Test
  public void testIndexFn2() throws Exception {
    // Test that we can index into a list that uses type coersion in its c'tor.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", new Utf8("a")));
    runFnTest("SELECT index(to_list('a',42), 0) AS x FROM memstream", checks);
  }

  @Test
  public void testContains1() throws Exception {
    // Test that we can use the contains() function normally.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.TRUE));
    runFnTest("SELECT contains(to_list(4,5,6), 4) AS x FROM memstream", checks);
  }

  @Test
  public void testContains2() throws Exception {
    // Test that we can use the contains() function normally.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.FALSE));
    runFnTest("SELECT contains(to_list(4,5,6), 7) AS x FROM memstream", checks);
  }

  @Test
  public void testContainsTypeCast() throws Exception {
    // Test that we can use the contains() function with type casting.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.FALSE));
    runFnTest("SELECT contains(to_list(4,5,6), 'a') AS x FROM memstream", checks);
  }

  @Test
  public void testContainsNullList() throws Exception {
    // Test that we can use the contains() function with a null list argument..
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", null));
    runFnTest("SELECT contains(null, 4) AS x FROM memstream", checks);
  }

  @Test
  public void testEmptyListContains() throws Exception {
    // Test that we can use the contains() function with an empty list
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.FALSE));
    runFnTest("SELECT contains(to_list(), 5) AS x FROM memstream", checks);
  }

  @Test
  public void testNullOnlyListContains() throws Exception {
    // Test that we can use the contains() function with a list containing only nulls
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.FALSE));
    runFnTest("SELECT contains(to_list(null, null), 5) AS x FROM memstream", checks);
  }

  @Test
  public void testNullOnlyListContainsNull() throws Exception {
    // Test that we can use the contains() function with a list containing only nulls
    // and a 'null' for the argument.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Boolean.TRUE));
    runFnTest("SELECT contains(to_list(null, null), null) AS x FROM memstream", checks);
  }

  @Test
  public void testListSize1() throws Exception {
    // Test that we can use the size() function on a list and get back 0 for empty. 
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Integer.valueOf(0)));
    runFnTest("SELECT size(to_list()) AS x FROM memstream", checks);
  }

  @Test
  public void testListSize2() throws Exception {
    // Test that we can use the size() function on a list and get back null for a
    // null list.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", null));
    runFnTest("SELECT size(null) AS x FROM memstream", checks);
  }

  @Test
  public void testListSize3() throws Exception {
    // Test that we can use the size() function to accurately test a real list size.
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("x", Integer.valueOf(2)));
    runFnTest("SELECT size(to_list('a', 'b')) AS x FROM memstream", checks);
  }
}
