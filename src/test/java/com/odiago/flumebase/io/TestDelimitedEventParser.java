// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.io;

import org.testng.annotations.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.lang.Type;

import static org.testng.AssertJUnit.*;

public class TestDelimitedEventParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestDelimitedEventParser.class.getName());

  private Event makeEvent(String text) {
    EventImpl event = new EventImpl(text.getBytes());
    return event;
  }

  @Test
  public void testEmpty() throws Exception {
    Event e = makeEvent("");
    DelimitedEventParser ep = new DelimitedEventParser();
    ep.reset(e);

    try {
      ep.getColumn(0, Type.getPrimitive(Type.TypeName.INT));
      fail("Expected error retrieving missing column.");
    } catch (ColumnParseException cpe) {
      // expected; ok
    }
  }

  @Test
  public void testSingleCol() throws Exception {
    Event e = makeEvent("42");
    DelimitedEventParser ep = new DelimitedEventParser();
    ep.reset(e);

    int result = (Integer) ep.getColumn(0, Type.getPrimitive(Type.TypeName.INT));
    assertEquals(42, result);

    // retrieve the same column twice.
    int result2 = (Integer) ep.getColumn(0, Type.getPrimitive(Type.TypeName.INT));
    assertEquals(42, result2);

    // Reset this onto another instance.
    ep.reset(makeEvent("meep"));
    String strResult = (String) ep.getColumn(0, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("meep", strResult);

    try {
      Object unexpectedResult = ep.getColumn(1, Type.getPrimitive(Type.TypeName.STRING));
      LOG.info("Got unexpected result: " + unexpectedResult);
      fail("Expected error retrieving missing column");
    } catch (ColumnParseException cpe) {
      // expected; ok
    }
  }

  @Test
  public void testMultiCols() throws Exception {
    Event e = makeEvent("1,2,3,4");
    DelimitedEventParser ep = new DelimitedEventParser();
    ep.reset(e);

    // Open all the columns in-order, assert that they parse correctly.
    for (int i = 0; i < 4; i++) {
      int result = (Integer) ep.getColumn(i, Type.getPrimitive(Type.TypeName.INT));
      LOG.info("Column i=" + i + "; result=" + result);
      assertEquals(i + 1, result);
    }

    // Try a scenario where we open the columns in reverse order, make sure
    // that caching works correctly.
    ep.reset(makeEvent("5,6,7,8"));
    for (int i = 0; i < 4; i++) {
      int col = 3 - i;
      int result = (Integer) ep.getColumn(col, Type.getPrimitive(Type.TypeName.INT));
      LOG.info("Column col=" + col + "; result=" + result);
      assertEquals(8 - i, result);
    }

    String s;

    // In-order parsing with strings.
    ep.reset(makeEvent("bar,quux"));
    s = (String) ep.getColumn(0, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("bar", s);
    s = (String) ep.getColumn(1, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("quux", s);

    // Go backward with strings.
    ep.reset(makeEvent("meep,foo"));
    s = (String) ep.getColumn(1, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("foo", s);
    s = (String) ep.getColumn(0, Type.getPrimitive(Type.TypeName.STRING));
    assertEquals("meep", s);

    // Ask for a column that does not exist; then go back and ask for one that does.
    ep.reset(makeEvent("1,2,3"));
    try {
      ep.getColumn(4, Type.getPrimitive(Type.TypeName.STRING));
      fail("Expected exception for non-existant 5th column");
    } catch (ColumnParseException cpe) {
      // expected.
    }

    int i = (Integer) ep.getColumn(1, Type.getPrimitive(Type.TypeName.INT));
    assertEquals(2, i);
  }
}
