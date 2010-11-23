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
}
