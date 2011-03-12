// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.lang;

/**
 * Exception thrown when a function evaluation fails to operate correctly.
 */
public class EvalException extends Exception {
  public EvalException() {
    super("EvalException");
  }

  public EvalException(String msg) {
    super(msg);
  }

  public EvalException(Throwable cause) {
    super(cause);
  }

  public EvalException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
