// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.antlr.runtime.Parser;
import org.antlr.runtime.RecognizerSharedState;
import org.antlr.runtime.TokenStream;

/**
 * Base parse tree representing a RTSQL statement.
 */
public abstract class AbstractSqlParse extends Parser {
  protected AbstractSqlParse(TokenStream input) {
    super(input);
  }

  protected AbstractSqlParse(TokenStream input, RecognizerSharedState state) {
    super(input, state);
  }
}
