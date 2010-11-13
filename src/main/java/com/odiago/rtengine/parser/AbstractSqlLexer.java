// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.Lexer;
import org.antlr.runtime.RecognizerSharedState;

/**
 * Base lexer class for parsing RTSQL.
 */
public abstract class AbstractSqlLexer extends Lexer {
  public AbstractSqlLexer() {
  }

  public AbstractSqlLexer(CharStream input) {
    this(input, new RecognizerSharedState());
  }

  public AbstractSqlLexer(CharStream input, RecognizerSharedState state) {
    super(input, state);

  }
}

