// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;

/**
 * Main interface to the parsing system.
 * The ASTGenerator accepts a string and returns the AST for the command
 * to execute.
 */
public class ASTGenerator {
  public ASTGenerator() {
  }

  public SQLStatement parse(String input) throws RecognitionException {
    SqlLexer lex = new SqlLexer(new ANTLRStringStream(input));
    CommonTokenStream tokens = new CommonTokenStream(lex);
    SqlGrammar parser = new SqlGrammar(tokens);

    return parser.top().val;
  }
}
