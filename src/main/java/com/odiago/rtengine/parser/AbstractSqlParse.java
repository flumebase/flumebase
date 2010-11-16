// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.PrintStream;

import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

/**
 * Base parse tree representing a RTSQL statement.
 */
public abstract class AbstractSqlParse extends Parser {
  /** Where do we send error information? */
  private PrintStream mErrPrintStream = System.err;

  protected AbstractSqlParse(TokenStream input) {
    super(input);
  }

  protected AbstractSqlParse(TokenStream input, RecognizerSharedState state) {
    super(input, state);
  }

  /**
   * Override the default behavior (write directly to stderr) with behavior
   * that allows us to write to the configured stream buffer of our choice.
   */
  @Override
  public void emitErrorMessage(String msg) {
    mErrPrintStream.println(msg);
  }

  /**
   * Specify the output stream where error messages are written.
   */
  public void setErrorStream(PrintStream errStream) {
    mErrPrintStream = errStream;
  }
}
