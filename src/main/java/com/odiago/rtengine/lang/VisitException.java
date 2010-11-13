// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

/**
 * Exception thrown when a visitor fails to operate correctly on the AST.
 */
public class VisitException extends Exception {
  public VisitException() {
    super("VisitException");
  }

  public VisitException(String msg) {
    super(msg);
  }

  public VisitException(Throwable cause) {
    super(cause);
  }

  public VisitException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
