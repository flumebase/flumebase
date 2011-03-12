// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.util;

/**
 * An exception signalling that an error occurred when processing a DAG node
 * with DAG.Operator.
 */
public class DAGOperatorException extends Exception {

  public DAGOperatorException() {
    super("DAGOperatorException");
  }

  public DAGOperatorException(String msg) {
    super(msg);
  }

  public DAGOperatorException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public DAGOperatorException(Throwable cause) {
    super(cause);
  }
}
