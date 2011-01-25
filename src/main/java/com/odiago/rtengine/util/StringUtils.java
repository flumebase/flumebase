// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.List;

/** Utility methods to help with string formatting, etc. */
public final class StringUtils {
  private StringUtils() {
  }

  /** Format members of a list into a comma-delimited string builder. */
  public static <E> void formatList(StringBuilder sb, List<E> lst) {
    boolean first = true;
    for (E elem : lst) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(elem.toString());
      first = false;
    }
  }

  /** Return a string containing the comma-delimited string representation of the input list. */
  public static <E> String listToStr(List<E> lst) {
    StringBuilder sb = new StringBuilder();
    formatList(sb, lst);
    return sb.toString();
  }

  /**
   * Given a possibly-qualified name like "foo.bar", return the unqualified name "bar".
   */
  public static String dequalify(String input) {
    int dotPos = input.indexOf(".");
    if (-1 == dotPos) {
      return input;
    } else {
      return input.substring(dotPos + 1);
    }
  }

  /**
   * Create a string representation of the specified throwable and its reified call stack.
   */
  public static String stringifyException(Throwable t) {
    // Hadoop already has this implementation; we call it from here so clients can import
    // a single StringUtils implementation.
    return org.apache.hadoop.util.StringUtils.stringifyException(t);
  }
}
