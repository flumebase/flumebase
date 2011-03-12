// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

import com.odiago.flumebase.exec.BuiltInSymbolTable;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.TypeChecker;

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
