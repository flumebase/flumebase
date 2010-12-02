// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

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
}
