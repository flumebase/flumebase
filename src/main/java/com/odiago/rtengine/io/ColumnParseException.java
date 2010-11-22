// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.io;

/**
 * An exception signalling that an error occurred when parsing a column of an event. 
 */
public class ColumnParseException extends Exception {

  public ColumnParseException() {
    super("ColumnParseException");
  }

  public ColumnParseException(String msg) {
    super(msg);
  }

  public ColumnParseException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public ColumnParseException(Throwable cause) {
    super(cause);
  }
}
