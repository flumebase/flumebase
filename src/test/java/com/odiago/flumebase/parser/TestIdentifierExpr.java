// (c) Copyright 2010 Odiago, Inc.

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
