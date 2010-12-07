// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;

import org.junit.Test;

import static org.junit.Assert.*;

import com.cloudera.flume.core.EventImpl;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.ParsingEventWrapper;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.io.DelimitedEventParser;

import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeChecker;
import com.odiago.rtengine.lang.VisitException;

public class TestIdentifierExpr extends ExprTestCase {

  @Test
  public void testIdentifiers() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;
    SymbolTable symbols = new HashSymbolTable();
    symbols.addSymbol(new Symbol("x", Type.getPrimitive(Type.TypeName.INT)));

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
