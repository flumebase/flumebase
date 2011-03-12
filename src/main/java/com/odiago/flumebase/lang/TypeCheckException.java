// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.lang;

/**
 * Exception thrown when type-checking the AST fails.
 */
public class TypeCheckException extends VisitException {
  public TypeCheckException() {
    super("TypeCheckException");
  }

  public TypeCheckException(String msg) {
    super(msg);
  }
}
