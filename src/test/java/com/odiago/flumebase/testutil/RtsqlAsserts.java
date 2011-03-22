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

package com.odiago.flumebase.testutil;

import static org.testng.AssertJUnit.*;

/**
 * More assertions we use.
 */
public final class RtsqlAsserts {

  public static void assertArrayEquals(int[] expected, int[] actual) {
    assertArrayEquals("Assertion failed!", expected, actual);
  }

  public static void assertArrayEquals(String message, int[] expected, int[] actual) {
    if (expected == null && actual != null) {
      fail(message + ": expected null array, got an actual array.");
    }

    if (expected != null && actual == null) {
      fail(message + ": got null actual array, expected non-null");
    }

    if (expected == null && actual == null) {
      return; // Both null; ok.
    }

    if (expected.length != actual.length) {
      fail(message + ": got length mismatch; actual len = " + actual.length
          + " expected " + expected.length);
    }

    for (int i = 0; i < expected.length; i++) {
      if (expected[i] != actual[i]) {
        fail(message + ": mismatch at position " + i + ", got " + actual[i]
            + " not " + expected[i]);
      }
    }
  }
}
