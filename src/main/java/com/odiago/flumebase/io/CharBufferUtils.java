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

import java.io.UnsupportedEncodingException;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.util.Utf8;

import org.apache.commons.lang.text.StrTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.lang.ListType;
import com.odiago.flumebase.lang.PreciseType;
import com.odiago.flumebase.lang.Timestamp;
import com.odiago.flumebase.lang.Type;

/**
 * Utility methods for parsing string-based values without
 * requiring that they be incorporated into a String object.
 */
public class CharBufferUtils {

  private static final Logger LOG = LoggerFactory.getLogger(
      CharBufferUtils.class.getName());

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  private CharBufferUtils() { }

  /**
   * Parse a CharSequence into a bool. Only the case-sensitive values
   * "true" and "false" are recongized; others result in a ColumnParseException.
   */
  public static boolean parseBool(CharSequence chars) throws ColumnParseException {
    if (TRUE_STR.contentEquals(chars)) {
      return true;
    } else if (FALSE_STR.contentEquals(chars)) {
      return false;
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Could not parse as boolean: " + chars);
      }
      throw new ColumnParseException("Invalid boolean");
    }
  }

  /**
   * Parses a CharSequence into an integer in base 10.
   */
  public static int parseInt(CharBuffer chars) throws ColumnParseException {
    int result = 0;
    
    final int limit = chars.limit();
    final int start = chars.position();
    if (0 == limit - start) {
      // The empty string can not be parsed as an integer.
      throw new ColumnParseException("No value provided");
    }
    
    boolean isNegative = false;
    for (int pos = start; pos < limit; pos++) {
      char cur = chars.get();
      if (pos == start && cur == '-') {
        isNegative = true;
        if (limit - start == 1) {
          // "-" is not an integer we accept.
          throw new ColumnParseException("No integer part provided");
        }
      } else if (Character.isDigit(cur)) {
        byte digitVal = (byte)( cur - '0' );
        result = result * 10 - digitVal;
        // TODO: Detect over/underflow and signal exception?
      } else {
        throw new ColumnParseException("Invalid character in number");
      }
    }

    // We built up the value as a negative, to use the larger "half" of the
    // integer range. If it's not negative, flip it on return.
    return isNegative ? result : -result;
  }

  /**
   * Parses a CharSequence into a long in base 10.
   */
  public static long parseLong(CharBuffer chars) throws ColumnParseException {
    long result = 0L;
    
    final int limit = chars.limit();
    final int start = chars.position();
    if (0 == limit - start) {
      // The empty string can not be parsed as an integer.
      throw new ColumnParseException("No value provided");
    }
    
    boolean isNegative = false;
    for (int pos = start; pos < limit; pos++) {
      char cur = chars.get();
      if (pos == start && cur == '-') {
        isNegative = true;
        if (limit - start == 1) {
          // "-" is not an integer we accept.
          throw new ColumnParseException("No integer part provided");
        }
      } else if (Character.isDigit(cur)) {
        byte digitVal = (byte)( cur - '0' );
        result = result * 10 - digitVal;
        // TODO: Detect over/underflow and signal exception?
      } else {
        throw new ColumnParseException("Invalid character in number");
      }
    }

    // We built up the value as a negative, to use the larger "half" of the
    // integer range. If it's not negative, flip it on return.
    return isNegative ? result : -result;
  }

  /**
   * Parses a CharSequence into a floating-point value.
   */
  public static float parseFloat(CharBuffer chars) throws ColumnParseException {
    try {
      return Float.valueOf(new String(chars.array()));
    } catch (NumberFormatException nfe) {
      throw new ColumnParseException(nfe);
    }
  }

  /**
   * Parses a CharSequence into a double-precision floating-point value.
   */
  public static double parseDouble(CharBuffer chars) throws ColumnParseException {
    try {
      return Double.valueOf(new String(chars.array()));
    } catch (NumberFormatException nfe) {
      throw new ColumnParseException(nfe);
    }
  }

  public static String parseString(CharBuffer chars) throws ColumnParseException {
    return chars.toString();
  }

  /**
   * Parses a CharSequence into a list of values, all of some other type.
   */
  public static List<Object> parseList(CharBuffer chars, Type listItemType,
      String nullStr, String listDelim) throws ColumnParseException {
    StrTokenizer tokenizer = new StrTokenizer(chars.toString(), listDelim.charAt(0));
    List<Object> out = new ArrayList<Object>();

    while (tokenizer.hasNext()) {
      String part = (String) tokenizer.next();
      out.add(parseType(CharBuffer.wrap(part), listItemType, nullStr, listDelim));
    }

    return Collections.unmodifiableList(out);
  }

  /**
   * Parses a CharSequence into a value of a given expected type.
   * @param chars the unparsed characters representing the value
   * @param expectedType the expected type of the final value
   * @param nullStr a token indicating a null String instance.
   */
  public static Object parseType(CharBuffer chars, Type expectedType,
      String nullStr, String listDelim) throws ColumnParseException {
    Type.TypeName primitiveTypeName = expectedType.getPrimitiveTypeName();

    // TODO(aaron): Test how this handles a field that is an empty string.
    Object out = null;
    switch (primitiveTypeName) {
    case BINARY:
      try {
        out = ByteBuffer.wrap(chars.toString().getBytes("UTF-8"));
      } catch (UnsupportedEncodingException uee) {
        // Shouldn't ever be able to get here.
        // (http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html)
        LOG.error("Your JVM doesn't support UTF-8. This is really, really bad.");
        throw new ColumnParseException(uee);
      }
      break;
    case BOOLEAN:
      out = CharBufferUtils.parseBool(chars);
      break;
    case INT:
      out = CharBufferUtils.parseInt(chars);
      break;
    case BIGINT:
      out = CharBufferUtils.parseLong(chars);
      break;
    case FLOAT:
      out = CharBufferUtils.parseFloat(chars);
      break;
    case DOUBLE:
      out = CharBufferUtils.parseDouble(chars);
      break;
    case STRING:
      String asStr = chars.toString();
      if (expectedType.isNullable() && asStr.equals(nullStr)) {
        out = null;
      } else {
        out = new Utf8(asStr);
      }
      break;
    case TIMESTAMP:
      out = CharBufferUtils.parseLong(chars);
      if (null != out) {
        out = new Timestamp((Long) out);
      }
      break;
    case TIMESPAN:
      // TODO: This should return a TimeSpan object, which is actually two
      // fields. We need to work on this... it should not just be a 'long'
      // representation.
      out = CharBufferUtils.parseLong(chars);
      break;
    case PRECISE:
      PreciseType preciseType = PreciseType.toPreciseType(expectedType);
      out = preciseType.parseStringInput(chars.toString());
      break;
    case LIST:
      out = parseList(chars, ListType.toListType(expectedType).getElementType(),
          nullStr, listDelim);
      break;
    default:
      throw new ColumnParseException("Cannot parse recursive types");
    }

    return out;
  }

}
