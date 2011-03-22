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

package com.odiago.flumebase.io;

import java.nio.CharBuffer;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

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
