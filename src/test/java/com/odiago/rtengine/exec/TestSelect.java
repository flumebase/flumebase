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

  @Test
  public void testSelectTwoFields() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b'; select both of them.

    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getPrimitive(Type.TypeName.INT)));
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
    QuerySubmitResponse response = env.submitQuery("SELECT a, b FROM memstream");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer expected = Integer.valueOf(i + 1);
      Integer negative = Integer.valueOf(-i - 1);
      GenericData.Record record = outRecords.get(i);
      assertEquals(expected, record.get("a"));
      assertEquals(negative, record.get("b"));
    }
  }

  @Test
  public void testProjection() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b'; select only the "b" field.

    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getPrimitive(Type.TypeName.INT)));
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
    QuerySubmitResponse response = env.submitQuery("SELECT a, b FROM memstream");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer negative = Integer.valueOf(-i - 1);
      GenericData.Record record = outRecords.get(i);
      assertEquals(negative, record.get("b"));
    }
  }

  @Test
  public void testRearrange() throws IOException, InterruptedException {
    // Populate a stream with fields 'a, b', select 'b, a'.
    // For good measure, put this entire query in UPPER CASE to ensure
    // we canonicalize case for stream, field names.

    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");

    streamBuilder.addField(new TypedField("a", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addField(new TypedField("b", Type.getPrimitive(Type.TypeName.INT)));
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
    QuerySubmitResponse response = env.submitQuery("SELECT B, A FROM MEMSTREAM");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = getOutput("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer expected = Integer.valueOf(i + 1);
      Integer negative = Integer.valueOf(-i - 1);
      GenericData.Record record = outRecords.get(i);
      assertEquals(expected, record.get("a"));
      assertEquals(negative, record.get("b"));
    }
  }

  // TODO: Write the following tests:
  //   Test "Case Sensitive Quoted Fields"
  //   Test "Case Sensitive Quoted Streams"
  //   Test that all the correct data comes back if we select the same field twice.
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
