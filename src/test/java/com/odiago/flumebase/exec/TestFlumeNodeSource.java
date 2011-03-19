// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.apache.avro.generic.GenericData;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

import com.odiago.flumebase.exec.local.LocalEnvironment;
import com.odiago.flumebase.exec.local.MemoryOutputElement;

import com.odiago.flumebase.flume.EmbeddedFlumeConfig;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.StreamSourceType;

import com.odiago.flumebase.testutil.RtsqlTestCase;
import com.odiago.flumebase.testutil.StreamBuilder;

import com.odiago.flumebase.util.concurrent.SelectableList;

/**
 * Tests that create logical nodes in Flume that act as remote;
 * these logical nodes are used as foreign streams through
 * local "-receiver" nodes.
 */
public class TestFlumeNodeSource extends RtsqlTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestFlumeNodeSource.class.getName());

  @Test(groups = {"slow"})
  public void testFlumeSource() throws Exception {
    // Create a flume node and use it as the source for a SELECT query.
    
    final String SRC_NODE = "srcnode";
    File sourceFile = File.createTempFile("records-", ".txt");
    sourceFile.deleteOnExit();
    String sourceFilename = sourceFile.getAbsolutePath();

    BufferedWriter w = new BufferedWriter(new FileWriter(sourceFile));
    try {
      w.write("line1\n");
      w.write("line2\n");
      w.write("line3\n");
    } finally {
      w.close();
    }

    EmbeddedFlumeConfig flumeConf = getFlumeConfig();
    String nodeSource = "text(\"" + sourceFilename + "\")";
    flumeConf.start();
    flumeConf.spawnLogicalNode(SRC_NODE, nodeSource, "console");

    StreamBuilder streamBuilder = new StreamBuilder("inputstream");
    streamBuilder.setSource(SRC_NODE);
    streamBuilder.setSourceType(StreamSourceType.Node);
    streamBuilder.setLocal(false);
    streamBuilder.addField("x", Type.getPrimitive(Type.TypeName.STRING));

    getSymbolTable().addSymbol(streamBuilder.build());
    LocalEnvironment env = getEnvironment();
    env.connect();

    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "select-out");

    QuerySubmitResponse selectResponse = env.submitQuery(
        "SELECT * FROM inputstream",
        getQueryOpts());
    assertNotNull(selectResponse);
    FlowId queryId = selectResponse.getFlowId();
    assertNotNull(queryId);

    MemoryOutputElement output = getOutput("select-out");
    assertNotNull(output);
    SelectableList<GenericData.Record> outRecords = output.getRecords();
    synchronized (outRecords) {
      while (outRecords.size() < 3) {
        outRecords.wait();
      }

      for(GenericData.Record r : outRecords) {
        LOG.debug("gotgot " + r.get("x"));
      }

      this.assertRecordExists(outRecords, "x", new Utf8("line1"));
      this.assertRecordExists(outRecords, "x", new Utf8("line2"));
      this.assertRecordExists(outRecords, "x", new Utf8("line3"));
    }
  }

  @Test(groups = { "slow" })
  public void testFlumeSourceTwice() throws Exception {
    // Create a flume node and use it as the source for two SELECT queries.
    // We destroy the first query before beginning the second one, so the
    // RtsqlMultiSink should only ever be used by one downstream sink at
    // a time.
    runDoubleQueryTest(true);
  } 

  @Test(groups = { "slow" })
  public void testFlumeSourceTwiceConcurrent() throws Exception {
    // Create a flume node and use it as the source for two SELECT queries.
    // The first query continues to run after the second one is started,
    // so we guarantee that the first query loses no data by the addition
    // of the second one.
    runDoubleQueryTest(false);
  } 

  private void runDoubleQueryTest(boolean killFirstQuery) throws Exception {
    final String SRC_NODE = "srcnode";
    File sourceFile = File.createTempFile("records-", ".txt");
    sourceFile.deleteOnExit();
    String sourceFilename = sourceFile.getAbsolutePath();

    EmbeddedFlumeConfig flumeConf = getFlumeConfig();
    String nodeSource = "tail(\"" + sourceFilename + "\")";
    flumeConf.start();
    flumeConf.spawnLogicalNode(SRC_NODE, nodeSource, "console");

    StreamBuilder streamBuilder = new StreamBuilder("inputstream");
    streamBuilder.setSource(SRC_NODE);
    streamBuilder.setSourceType(StreamSourceType.Node);
    streamBuilder.setLocal(false);
    streamBuilder.addField("x", Type.getPrimitive(Type.TypeName.STRING));
    streamBuilder.addField("y", Type.getPrimitive(Type.TypeName.INT));

    getSymbolTable().addSymbol(streamBuilder.build());
    LocalEnvironment env = getEnvironment();
    env.connect();

    // Test the first query.
    LOG.debug("Running first query");
    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "select-out");

    QuerySubmitResponse selectResponse1 = env.submitQuery(
        "SELECT * FROM inputstream",
        getQueryOpts());
    assertNotNull(selectResponse1);
    FlowId queryId1 = selectResponse1.getFlowId();
    assertNotNull(queryId1);

    // Put some data into the stream.
    BufferedWriter w = new BufferedWriter(new FileWriter(sourceFile));
    try {
      w.write("line1,1\n");
      w.write("line2,2\n");
      w.write("line3,3\n");
    } finally {
      w.close();
    }

    MemoryOutputElement output1 = getOutput("select-out");
    assertNotNull(output1);
    SelectableList<GenericData.Record> outRecords1 = output1.getRecords();
    synchronized (outRecords1) {
      while (outRecords1.size() < 3) {
        outRecords1.wait();
      }

      for(GenericData.Record r : outRecords1) {
        LOG.debug("query 1 got " + r.get("x"));
      }

      this.assertRecordExists(outRecords1, "x", new Utf8("line1"));
      this.assertRecordExists(outRecords1, "x", new Utf8("line2"));
      this.assertRecordExists(outRecords1, "x", new Utf8("line3"));
    }

    LOG.debug("first query SUCCESS");

    if (killFirstQuery) {
      LOG.debug("Canceling first query");
      env.cancelFlow(queryId1);
      joinFlow(queryId1);
      LOG.debug("First query canceled.");
    }

    // Test the second query.
    LOG.debug("Running second query");
    getConf().set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "select-out2");

    QuerySubmitResponse selectResponse2 = env.submitQuery(
        "SELECT * FROM inputstream WHERE y = 6",
        getQueryOpts());
    assertNotNull(selectResponse2);
    FlowId queryId2 = selectResponse2.getFlowId();
    assertNotNull(queryId2);

    // Add more data to the stream.
    w = new BufferedWriter(new FileWriter(sourceFile, true));
    try {
      w.write("line4,4\n");
      w.write("line5,5\n");
      w.write("line6,6\n");
    } finally {
      w.close();
    }
    
    MemoryOutputElement output2 = getOutput("select-out2");
    assertNotNull(output2);
    SelectableList<GenericData.Record> outRecords2 = output2.getRecords();
    synchronized (outRecords2) {
      while (outRecords2.size() < 1) {
        outRecords2.wait();
      }

      for(GenericData.Record r : outRecords2) {
        LOG.debug("query 2 got " + r.get("x"));
      }

      this.assertRecordFields(outRecords2, "y", Integer.valueOf(6), "x", new Utf8("line6"));
    }
    LOG.debug("second query SUCCESS");

    if (!killFirstQuery) {
      // Double check to ensure that the first query received those additional records.
      synchronized (outRecords1) {
        while (outRecords1.size() < 6) {
          outRecords1.wait();
        }

        for(GenericData.Record r : outRecords1) {
          LOG.debug("second time around, query 1 got " + r.get("x"));
        }

        this.assertRecordExists(outRecords1, "x", new Utf8("line4"));
        this.assertRecordExists(outRecords1, "x", new Utf8("line5"));
        this.assertRecordExists(outRecords1, "x", new Utf8("line6"));
      }

      LOG.debug("first query CONTINUED SUCCESS");
      LOG.debug("Now we're killing the first query.");
      env.cancelFlow(queryId1);
      joinFlow(queryId1);
    }

    LOG.debug("Killing 2nd query.");
    env.cancelFlow(queryId2);
    joinFlow(queryId2);
  }
}
