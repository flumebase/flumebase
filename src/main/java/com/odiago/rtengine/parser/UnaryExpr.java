// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.List;

import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * Unary operator expression (negation, logical not).
 */
public class UnaryExpr extends Expr {
  private UnaryOp mOp;
  private Expr mSubExpr;

  public UnaryExpr(UnaryOp op, Expr subExpr) {
    mOp = op;
    mSubExpr = subExpr;
  }

  public UnaryOp getOp() {
    return mOp;
  }

  public Expr getSubExpr() {
    return mSubExpr;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("UnaryExpr mOp=");
    sb.append(mOp);
    sb.append("\n");
    mSubExpr.format(sb, depth + 1);
  }

  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(symbolForOp(mOp));
    sb.append(mSubExpr.toStringOneLine());
    return sb.toString();
  }

  private static String symbolForOp(UnaryOp op) {
    switch (op) {
    case Plus:
      return "+";
    case Minus:
      return "-";
    case Not:
      return "NOT";
    default:
      throw new RuntimeException("symbolForOp does not understand " + op);
    }
  }

  @Override
  public Type getType(SymbolTable symTab) {
    switch (mOp) {
    case Plus:
    case Minus:
      // Numeric operators return their input type.
      return mSubExpr.getType(symTab);
    case Not:
      // logical operator returns boolean.
      return Type.getPrimitive(Type.TypeName.BOOLEAN);
    default:
      throw new RuntimeException("Unary getType() cannot handle operator " + mOp);
    }
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    return this.mSubExpr.getRequiredFields(symTab);
  }

}
