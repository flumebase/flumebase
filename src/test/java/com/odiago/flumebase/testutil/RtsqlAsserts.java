// (c) Copyright 2011 Odiago, Inc.

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
