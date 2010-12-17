// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Represents two sources to a SELECT statement, married by a (windowed) JOIN clause.
 */
public class JoinedSource extends SQLStatement {

  private SQLStatement mLeftSrc;
  private SQLStatement mRightSrc;
  private Expr mJoinExpr;
  private Expr mWindowExpr;

  /**
   * Specifies a statement of the form "... &lt;leftSrc&gt; JOIN &lt;rightSrc&gt; ON ..."
   * @param leftSrc the "primary" source.
   * @param rightSrc the "secondary" source being joined in to the primary.
   * @param joinExpr the expression which must evaluate to true for the join to hold.
   * @param windowExpr an expression that resolves to a window specification, defining
   * the temporal boundaries of the join.
   */
  public JoinedSource(SQLStatement leftSrc, SQLStatement rightSrc, Expr joinExpr,
      Expr windowExpr) {
    mLeftSrc = leftSrc;
    mRightSrc = rightSrc;
    mJoinExpr = joinExpr;
    mWindowExpr = windowExpr;
  }

  public SQLStatement getLeft() {
    return mLeftSrc;
  }

  public SQLStatement getRight() {
    return mRightSrc;
  }

  public Expr getJoinExpr() {
    return mJoinExpr;
  }

  public Expr getWindowExpr() {
    return mWindowExpr;
  }
  
  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("JOIN\n");
    pad(sb, depth + 1);
    sb.append("left:\n");
    mLeftSrc.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("right:\n");
    mRightSrc.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("ON:\n");
    mJoinExpr.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("OVER:\n");
    mWindowExpr.format(sb, depth + 2);
  }

}
