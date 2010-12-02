// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Binary operator expression.
 */
public class BinExpr extends Expr {
  private BinOp mOp;
  private Expr mLeftExpr;
  private Expr mRightExpr;

  public BinExpr(Expr leftExpr, BinOp op, Expr rightExpr) {
    mLeftExpr = leftExpr;
    mOp = op;
    mRightExpr = rightExpr;
  }

  public BinOp getOp() {
    return mOp;
  }

  public Expr getLeftExpr() {
    return mLeftExpr;
  }

  public Expr getRightExpr() {
    return mRightExpr;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("BinExpr mOp=");
    sb.append(mOp);
    sb.append("\n");
    pad(sb, depth);
    sb.append("left expr:\n");
    mLeftExpr.format(sb, depth + 1);
    sb.append("right expr:\n");
    mRightExpr.format(sb, depth + 1);
  }
}
