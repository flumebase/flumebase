// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import java.io.PrintStream;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognizerSharedState;

/**
 * Base lexer class for parsing RTSQL.
 */
public abstract class AbstractSqlLexer extends Lexer {
  /** Where do we send error information? */
  private PrintStream mErrPrintStream = System.err;

  public AbstractSqlLexer() {
  }

  public AbstractSqlLexer(CharStream input) {
    this(input, new RecognizerSharedState());
  }

  public AbstractSqlLexer(CharStream input, RecognizerSharedState state) {
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

