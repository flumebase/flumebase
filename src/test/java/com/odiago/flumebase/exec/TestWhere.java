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
import java.util.List;

import org.apache.avro.generic.GenericData;

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
 * Test that SELECT statements with WHERE clauses operate like we expect them to.
 */
public class TestWhere extends RtsqlTestCase {

  /**
   * Run a test where one records of two integer-typed fields is selected from
   * two input records.
   * @param streamName the stream to create and populate with two records.
   * @param query the query string to submit to the execution engine.
   * @param checkFields a list of (string, object) pairs naming an output field and
   * its value to verify.
   */
  private void runWhereTest(String streamName, String query,
      List<Pair<String, Object>> checkFields)
      throws IOException, InterruptedException {
    MemStreamBuilder streamBuilder = new MemStreamBuilder(streamName);

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getNullable(Type.TypeName.INT)));
    streamBuilder.addEvent("1,2");
    streamBuilder.addEvent("3,4");
    StreamSymbol stream = streamBuilder.build();
    getSymbolTable().addSymbol(stream);

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
  public void testWhere1() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));
    runWhereTest("memstream", "SELECT a, b FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testWhere2() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));
    runWhereTest("memstream", "SELECT * FROM memstream WHERE a = 1", checks);
  }

  @Test
  public void testWhere3() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    checks.add(new Pair<String, Object>("b", Integer.valueOf(2)));
    runWhereTest("memstream", "SELECT * FROM memstream WHERE a = 1 and b = 2", checks);
  }

  @Test
  public void testWhere4() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(1)));
    runWhereTest("memstream", "SELECT a FROM memstream WHERE b = 2", checks);
  }

  @Test
  public void testWhere5() throws IOException, InterruptedException {
    List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
    checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
    runWhereTest("memstream", "SELECT a FROM memstream WHERE a > 2", checks);
  }

  @Test
  public void testWhere6() throws IOException, InterruptedException {
    boolean failed = false;

    try {
      List<Pair<String, Object>> checks = new ArrayList<Pair<String, Object>>();
      checks.add(new Pair<String, Object>("a", Integer.valueOf(3)));
      runWhereTest("memstream", "SELECT a FROM memstream WHERE 42", checks);
      failed = true;
    } catch (AssertionError ae) {
      // Expected; 42 is not a boolean, so this should not type check.
    }

    if (failed) {
      fail("Expected type checker error!");
    }
  }

}
