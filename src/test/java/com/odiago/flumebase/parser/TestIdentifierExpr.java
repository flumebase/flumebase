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

import java.util.ArrayList;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.ParsingEventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.io.DelimitedEventParser;

import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.TypeChecker;
import com.odiago.flumebase.lang.VisitException;

public class TestIdentifierExpr extends ExprTestCase {

  @Test
  public void testIdentifiers() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;
    SymbolTable symbols = new HashSymbolTable();
    symbols.addSymbol(new AssignedSymbol("x", Type.getPrimitive(Type.TypeName.INT), "x"));

    ArrayList<String> symbolNames = new ArrayList<String>();
    symbolNames.add("x");
    EventWrapper wrapper = new ParsingEventWrapper(new DelimitedEventParser(),
        symbolNames);
    wrapper.reset(new EventImpl("4".getBytes()));

    // This is expected to succeed.
    binExpr = new BinExpr(
        new IdentifierExpr("x"),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(symbols);
    binExpr.accept(checker);
    value = binExpr.eval(wrapper);
    assertEquals(Integer.valueOf(8), value);

    // This should fail; 'y' is not a registered symbol.
    try {
      binExpr = new BinExpr(
          new IdentifierExpr("y"),
          BinOp.Times,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
      checker = new TypeChecker(symbols);
      binExpr.accept(checker);
      fail("Expected type checker error; no symbol 'y'.");
    } catch (VisitException ve) {
      // expected; ok.
    }
  }
}
