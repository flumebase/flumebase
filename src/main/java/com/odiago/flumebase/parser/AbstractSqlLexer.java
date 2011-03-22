/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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

