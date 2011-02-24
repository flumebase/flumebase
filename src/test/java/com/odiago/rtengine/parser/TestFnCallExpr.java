// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

import com.odiago.rtengine.exec.BuiltInSymbolTable;
import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeChecker;

public class TestFnCallExpr extends ExprTestCase {

  @Test
  public void testLengthFn() throws Exception {
    FnCallExpr fnCallExpr;
    TypeChecker checker;
    Object value;
    SymbolTable symbols = new HashSymbolTable(new BuiltInSymbolTable());

    // This is expected to succeed.
    fnCallExpr = new FnCallExpr("length");
    fnCallExpr.addArg(new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "meep"));
    checker = new TypeChecker(symbols);
    fnCallExpr.accept(checker);
    value = fnCallExpr.eval(getEmptyEventWrapper());

    assertEquals(Integer.valueOf(4), value);
  }
}
