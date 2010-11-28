// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericData;

import org.apache.hadoop.conf.Configuration;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.local.LocalEnvironment;
import com.odiago.rtengine.exec.local.MemoryOutputElement;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.testutil.MemStreamBuilder;

import static org.junit.Assert.*;

/**
 * Test that SELECT statements operate like we expect them to.
 */
public class TestSelect {
  private static final Logger LOG = LoggerFactory.getLogger(TestSelect.class.getName());

  @Test
  public void testSelectOneCol() throws IOException, InterruptedException {
    // Given three input records with a single column, select the column
    // and get the same result back.

    // Create the stream, and put some records in it.
    MemStreamBuilder streamBuilder = new MemStreamBuilder("memstream");
    SymbolTable symtab = new HashSymbolTable();

    streamBuilder.addField(new TypedField("fieldname", Type.getPrimitive(Type.TypeName.INT)));
    streamBuilder.addEvent("1");
    streamBuilder.addEvent("2");
    streamBuilder.addEvent("3");
    StreamSymbol stream = streamBuilder.build();
    LOG.debug("Input memory stream: " + stream);
    symtab.addSymbol(stream);

    Configuration conf = new Configuration();
    conf.set(SelectStmt.CLIENT_SELECT_TARGET_KEY, "testSelect");

    Map<String, MemoryOutputElement> outputMap =
      Collections.synchronizedMap(new HashMap<String, MemoryOutputElement>());

    // Now create a LocalEnvironment around this pre-populated symbol table.
    LocalEnvironment env = new LocalEnvironment(conf, symtab, outputMap);

    env.connect();

    // Run the query
    QuerySubmitResponse response = env.submitQuery("SELECT fieldname FROM memstream");
    FlowId id = response.getFlowId();
    assertNotNull(id);
    env.joinFlow(id);

    // Examine the response records.
    MemoryOutputElement output = outputMap.get("testSelect");
    assertNotNull(output);

    List<GenericData.Record> outRecords = output.getRecords();
    for (int i = 0; i < 3; i++) {
      Integer expected = Integer.valueOf(i + 1);
      GenericData.Record record = outRecords.get(i);
      assertEquals(expected, record.get("fieldname"));
    }

    // Cleanup..
    env.disconnect();
  }
  
}
