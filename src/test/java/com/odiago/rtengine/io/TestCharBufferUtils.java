// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.io;

import java.nio.CharBuffer;

import org.junit.Test;

import static junit.framework.Assert.*;

public class TestCharBufferUtils {
  private CharBuffer makeCharBuffer(String text) {
    return CharBuffer.wrap(text.toCharArray());
  }

  @Test
  public void testBooleans() throws Exception {
    boolean result = CharBufferUtils.parseBool("true");
    assertEquals(true, result);

    result = CharBufferUtils.parseBool("false");
    assertEquals(false, result);

    try {
      // Only the two strings above are recognized; this one should fail
      // with an exception.
      CharBufferUtils.parseBool("True");
      fail("Expected ColumnParseException");
    } catch (ColumnParseException e) {
      // expected; ok.
    }
  }

  @Test
  public void testInts() throws Exception {
    int result = CharBufferUtils.parseInt(makeCharBuffer("42"));
    assertEquals(42, result);

    result = CharBufferUtils.parseInt(makeCharBuffer("0"));
    assertEquals(0, result);

    result = CharBufferUtils.parseInt(makeCharBuffer("5"));
    assertEquals(5, result);

    result = CharBufferUtils.parseInt(makeCharBuffer("-42"));
    assertEquals(-42, result);

    // Test a character buffer that starts at an offset into an array.
    char[] someChars = { '0', ',', '2' };
    CharBuffer offsetCharBuffer = CharBuffer.wrap(someChars, 2, 1);
    result = CharBufferUtils.parseInt(offsetCharBuffer);
    assertEquals(2, result);

    try {
      CharBufferUtils.parseInt(makeCharBuffer(""));
      fail("Expected ColumnParseException");
    } catch (ColumnParseException e) {
      // expected; ok.
    }

    try {
      CharBufferUtils.parseInt(makeCharBuffer("-"));
      fail("Expected ColumnParseException");
    } catch (ColumnParseException e) {
      // expected; ok.
    }
  }
}
