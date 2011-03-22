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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.avro.generic.GenericData;

import static org.testng.AssertJUnit.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.local.LocalEnvironment;
import com.odiago.flumebase.exec.local.MemoryOutputElement;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.FormatSpec;
import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.StreamSourceType;
import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.testutil.RtsqlTestCase;

import org.testng.annotations.Test;

import com.odiago.flumebase.testutil.StreamBuilder;

import com.odiago.flumebase.util.concurrent.SelectableList;

/**
 * Test the ability to CREATE STREAM AS SELECT and then
 * select from it into another query.
 */
public class TestStreamOutput extends RtsqlTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestStreamOutput.class.getName());

  @Test(groups = { "slow" })
  public void testCreateAsSelect() throws Exception {
    // Create a stream we will fill with numbers via flume.
    File sourceFile = File.createTempFile("numberstream-", ".txt");
    sourceFile.deleteOnExit();
    String sourceFilename = sourceFile.getAbsolutePath();

    StreamBuilder streamBuilder = new StreamBuilder("inputstream");

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.setFormat(new FormatSpec("delimited"));
    streamBuilder.setLocal(true);
    streamBuilder.setSourceType(StreamSourceType.Source);
    streamBuilder.setSource("tail(\"" + sourceFilename + "\")");
    StreamSymbol inputStream = streamBuilder.build();

    getSymbolTable().addSymbol(inputStream);

    LocalEnvironment env = getEnvironment();
    env.connect();

    // Create another stream that selects 2 * any value we put into 'inputstream'.
    QuerySubmitResponse createResponse = env.submitQuery(
        "CREATE STREAM doubled AS SELECT 2 * a as b FROM inputstream",
        getQueryOpts());
    LOG.info("Create response message: " + createResponse.getMessage());
    FlowId createId = createResponse.getFlowId();
    assertNotNull(createId);

    // Select 3 * any value in doubled.

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "test6x");
    QuerySubmitResponse queryResponse = env.submitQuery(
        "SELECT 3 * b as c FROM doubled", getQueryOpts());
    LOG.info("Query response message: " + queryResponse.getMessage());
    FlowId queryId = queryResponse.getFlowId();
    assertNotNull(queryId);

    // Push several values into the tail file.
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sourceFile)));
      for (int i = 0; i < 3; i++) {
        writer.write("" + i + "\n");
      }
    } finally {
      if (null != writer) {
        try {
          writer.close();
        } catch (IOException ioe) {
          LOG.error("IOE closing writer: " + ioe);
        }
      }
    }

    // Check that we received several multiples of six out.
    MemoryOutputElement output = getOutput("test6x");
    assertNotNull(output);

    // Wait until we have received the expected amout of output data.
    // Flume processing is asynchronous, so we need to wait until all
    // our inputs have reached the output.
    SelectableList<GenericData.Record> outRecords = output.getRecords();
    synchronized (outRecords) {
      while (outRecords.size() < 3) {
        outRecords.wait();
      }

      for (int i = 0; i < 3; i++) {
        Integer expected = Integer.valueOf(i * 6);
        GenericData.Record record = outRecords.get(i);
        assertEquals(expected, record.get("c"));
      }
    }
  }
}
