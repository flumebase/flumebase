// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.junit.Test;

import static org.junit.Assert.*;

import com.odiago.rtengine.exec.HashSymbolTable;

import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeCheckException;
import com.odiago.rtengine.lang.TypeChecker;
import com.odiago.rtengine.lang.VisitException;

public class TestBinExpr extends ExprTestCase {

  @Test
  public void testTimes() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(8), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(-2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(-8), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(4)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Long.valueOf(8), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Long.valueOf(8), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Long.valueOf(4)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Long.valueOf(8), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(12f)),
        BinOp.Times,
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(2.5f)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Float.valueOf(12f * 2.5f), value);

    try {
      binExpr = new UnaryExpr(UnaryOp.Not,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(42)));
      checker = new TypeChecker(new HashSymbolTable());
      binExpr.accept(checker);
      fail("Expected typechecker error on NOT(INTEGER)");
    } catch (TypeCheckException tce) {
      // expected this -- ok.
    }
  }

  @Test
  public void testPlus() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Add,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(6), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Add,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(-2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(2), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.Add,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "def"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals("abcdef", value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.Add,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals("abc3", value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)),
        BinOp.Add,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "xyz"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals("3xyz", value);
  }

  @Test
  public void testMinus() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Subtract,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(2), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Subtract,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(-2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(6), value);
  }

  @Test
  public void testDiv() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(6)),
        BinOp.Div,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(3), value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(7f)),
        BinOp.Div,
        new ConstExpr(Type.getPrimitive(Type.TypeName.FLOAT), Float.valueOf(2f)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Float.valueOf(7f/2f), value);
  }

  @Test
  public void testMod() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.Mod,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Integer.valueOf(1), value);
  }

  @Test
  public void testGreater() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.Greater,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
        BinOp.Greater,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Greater,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    // Note that booleans are comparable..
    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE),
        BinOp.Greater,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);
  }

  @Test
  public void testGreaterEq() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.GreaterEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
        BinOp.GreaterEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.GreaterEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);
  }

  @Test
  public void testLessEq() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.LessEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
        BinOp.LessEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.LessEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);
  }

  @Test
  public void testLess() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.Less,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
        BinOp.Less,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Less,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);
  }

  @Test
  public void testEq() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getNullable(Type.TypeName.INT), null),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertNull(value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "ABC"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    // integer-to-string promotion holds for the '=' operator too.

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "4"),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.Eq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "4"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);
  }

  @Test
  public void testNotEq() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(7)),
        BinOp.NotEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)),
        BinOp.NotEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getNullable(Type.TypeName.INT), null),
        BinOp.NotEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertNull(value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.NotEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "abc"),
        BinOp.NotEq,
        new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), "ABC"));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);
  }

  @Test
  public void testAnd() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE),
        BinOp.And,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
        BinOp.And,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE),
        BinOp.And,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
        BinOp.And,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    try {
      // This should cause a typechecker exception.
      binExpr = new BinExpr(
          new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
          BinOp.And,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
      checker = new TypeChecker(new HashSymbolTable());
      binExpr.accept(checker);

      fail("Expected typechecker error ; int does not promote to boolean.");
    } catch (VisitException ve) {
      // Expected.
    }
  }

  @Test
  public void testOr() throws Exception {
    Expr binExpr;
    TypeChecker checker;
    Object value;

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE),
        BinOp.Or,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
        BinOp.Or,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE),
        BinOp.Or,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.TRUE, value);

    binExpr = new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
        BinOp.Or,
        new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE));
    checker = new TypeChecker(new HashSymbolTable());
    binExpr.accept(checker);
    value = binExpr.eval(getEmptyEventWrapper());
    assertEquals(Boolean.FALSE, value);

    try {
      // This should cause a typechecker exception.
      binExpr = new BinExpr(
          new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE),
          BinOp.Or,
          new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4)));
      checker = new TypeChecker(new HashSymbolTable());
      binExpr.accept(checker);

      fail("Expected typechecker error ; int does not promote to boolean.");
    } catch (VisitException ve) {
      // Expected.
    }
  }
}
