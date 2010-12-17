// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import org.junit.Test;

import com.odiago.rtengine.exec.AssignedSymbol;
import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.BinOp;
import com.odiago.rtengine.parser.ConstExpr;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.IdentifierExpr;

public class TestTypeChecker {

  @Test
  public void testBasicBinop() throws VisitException {
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)));
    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test
  public void testNestedBinop() throws VisitException {
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, 
      new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)),
        BinOp.Add, 
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(4))));

    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test(expected=VisitException.class)
  public void testBasicBinopFail() throws VisitException {
    // can't add INT and TIMESTAMP.
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, new ConstExpr(Type.getPrimitive(Type.TypeName.TIMESTAMP), Integer.valueOf(3)));
    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test(expected=VisitException.class)
  public void testNestedBinopFail() throws VisitException {
    // can't add INT and TIMESTAMP in a subexpr.
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, 
      new BinExpr(
        new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)),
        BinOp.Add, 
        new ConstExpr(Type.getPrimitive(Type.TypeName.TIMESTAMP), Integer.valueOf(4))));

    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test
  public void testPromotion1() throws VisitException {
    // Test that INT can promote to BIGINT.
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Integer.valueOf(3)));
    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test
  public void testPromotion2() throws VisitException {
    // Test that INT can promote to BIGINT on the lhs.
    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Integer.valueOf(2)),
      BinOp.Add, new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(3)));
    TypeChecker tc = new TypeChecker(new HashSymbolTable());
    binopExpr.accept(tc);
  }

  @Test
  public void testIdentifier() throws VisitException {
    // Test that we can look up an identifier in the symbol table.
    SymbolTable symbols = new HashSymbolTable();
    symbols.addSymbol(new AssignedSymbol("x", Type.getPrimitive(Type.TypeName.INT), "x"));

    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, new IdentifierExpr("x"));
    TypeChecker tc = new TypeChecker(symbols);
    binopExpr.accept(tc);
  }

  @Test
  public void testIdentifierPromotion() throws VisitException {
    // Test that an identifier's type can promote to a constant.
    SymbolTable symbols = new HashSymbolTable();
    symbols.addSymbol(new AssignedSymbol("x", Type.getPrimitive(Type.TypeName.INT), "x"));

    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.BIGINT), Integer.valueOf(2)),
      BinOp.Add, new IdentifierExpr("x"));
    TypeChecker tc = new TypeChecker(symbols);
    binopExpr.accept(tc);
  }

  @Test
  public void testIdentifierPromotion2() throws VisitException {
    // Test that a const's type can promote to an identifier's.
    SymbolTable symbols = new HashSymbolTable();
    symbols.addSymbol(new AssignedSymbol("x", Type.getPrimitive(Type.TypeName.BIGINT), "x"));

    Expr binopExpr = new BinExpr(
      new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(2)),
      BinOp.Add, new IdentifierExpr("x"));
    TypeChecker tc = new TypeChecker(symbols);
    binopExpr.accept(tc);
  }

  // TODO:
  // Test unary expressions
  // Test restrictions on aliasedexpr with AllFieldsExpr
  // Test select stmt, inferring field types from a stream in the symboltable.
}
