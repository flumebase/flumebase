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

package com.odiago.flumebase.util;

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
      if (null == elem) {
        sb.append("null");
      } else {
        sb.append(elem.toString());
      }
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
