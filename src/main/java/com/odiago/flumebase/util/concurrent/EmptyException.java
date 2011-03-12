// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util.concurrent;

public class EmptyException extends Exception {
  public EmptyException() {
    super("EmptyException");
  }

  public EmptyException(Throwable t) {
    super("EmptyException", t);
  }
}
