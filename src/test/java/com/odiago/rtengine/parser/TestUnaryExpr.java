// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.junit.Test;

import static org.junit.Assert.*;

import com.odiago.rtengine.exec.HashSymbolTable;

import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeCheckException;
import com.odiago.rtengine.lang.TypeChecker;

public class TestUnaryExpr extends ExprTestCase {

  @Test
  public void testNot() throws Exception {
    Expr unaryExpr;
    TypeChecker checker;
    Object value;

    unaryExpr = new UnaryExpr(UnaryOp.Not,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    unaryExpr = new UnaryExpr(UnaryOp.Not,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    unaryExpr = new UnaryExpr(UnaryOp.Not,
        new UnaryExpr(UnaryOp.Not,
          new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    try {
      unaryExpr = new UnaryExpr(UnaryOp.Not,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(42)));
      checker = new TypeChecker(new HashSymbolTable());
      unaryExpr.accept(checker);
      fail("Expected typechecker error on NOT(INTEGER)");
    } catch (TypeCheckException tce) {
      // expected this -- ok.
    }
  }

  @Test
  public void testNegate() throws Exception {
    Expr unaryExpr;
    TypeChecker checker;
    Object value;

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(10)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(-10), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(0)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(0), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Long.valueOf(42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(-42f)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Float.valueOf(42f), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.DOUBLE), Double.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Double.valueOf(42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Minus,
        new UnaryExpr(UnaryOp.Minus,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(12))));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(12), value);

    try {
      unaryExpr = new UnaryExpr(UnaryOp.Minus,
          new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "hello"));
      checker = new TypeChecker(new HashSymbolTable());
      unaryExpr.accept(checker);
      fail("Expected typechecker error on -(STRING)");
    } catch (TypeCheckException tce) {
      // expected this -- ok.
    }
  }

  @Test
  public void testPositive() throws Exception {
    Expr unaryExpr;
    TypeChecker checker;
    Object value;

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(10)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(10), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(-42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(0)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(0), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Long.valueOf(-42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(-42f)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Float.valueOf(-42f), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new ConstExpr(Type.getPrimitive(Type.TypeName.DOUBLE), Double.valueOf(-42)));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Double.valueOf(-42), value);

    unaryExpr = new UnaryExpr(UnaryOp.Plus,
        new UnaryExpr(UnaryOp.Plus,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(12))));
    checker = new TypeChecker(new HashSymbolTable());
    unaryExpr.accept(checker);
    value = unaryExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(12), value);

    try {
      unaryExpr = new UnaryExpr(UnaryOp.Plus,
          new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "hello"));
      checker = new TypeChecker(new HashSymbolTable());
      unaryExpr.accept(checker);
      fail("Expected typechecker error on +(STRING)");
    } catch (TypeCheckException tce) {
      // expected this -- ok.
    }
  }
}
