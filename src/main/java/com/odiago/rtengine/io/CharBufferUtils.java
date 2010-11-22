// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.io;

import java.nio.CharBuffer;

/**
 * Utility methods for parsing string-based values without
 * requiring that they be incorporated into a String object.
 */
public class CharBufferUtils {

  private CharBufferUtils() { }

  private static final String TRUE_STR = "true";
  private static final String FALSE_STR = "false";

  /**
   * Parse a CharSequence into a bool. Only the case-sensitive values
   * "true" and "false" are recongized; others result in a ColumnParseException.
   */
  public static boolean parseBool(CharSequence chars) throws ColumnParseException {
    if (TRUE_STR.contentEquals(chars)) {
      return true;
    } else if (FALSE_STR.contentEquals("chars")) {
      return false;
    } else {
      throw new ColumnParseException("Invalid boolean");
    }
  }

  /**
   * Parses a CharSequence into an integer in base 10.
   */
  public static int parseInt(CharBuffer chars) throws ColumnParseException {
    int result = 0;
    
    final int len = chars.length();
    final int start = chars.position();
    boolean isNegative = false;
    for (int pos = start; pos < len; pos++) {
      char cur = chars.get();
      if (pos == start && cur == '-') {
        isNegative = true;
      } else if (Character.isDigit(cur)) {
        byte digitVal = (byte)( cur - '0' );
        result = result * 10 + digitVal;
        if (isNegative) {
          result = 0 - result; // Flip negative after first value added in.
          isNegative = false; // Only process this once.
        }
        // TODO: Detect over/underflow and signal exception?
      } else {
        throw new ColumnParseException("Invalid character in number");
      }
    }

    return result;
  }

  /**
   * Parses a CharSequence into a long in base 10.
   */
  public static long parseLong(CharBuffer chars) throws ColumnParseException {
    long result = 0L;
    
    final int len = chars.length();
    final int start = chars.position();
    boolean isNegative = false;
    for (int pos = start; pos < len; pos++) {
      char cur = chars.get();
      if (pos == start && cur == '-') {
        isNegative = true;
      } else if (Character.isDigit(cur)) {
        byte digitVal = (byte)( cur - '0' );
        result = result * 10 + digitVal;
        if (isNegative) {
          result = 0 - result; // Flip negative after first value added in.
          isNegative = false; // Only process this once.
        }
        // TODO: Detect over/underflow and signal exception?
      } else {
        throw new ColumnParseException("Invalid character in number");
      }
    }

    return result;
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
    return new String(chars.array(), chars.position(), chars.length());
  }

}
