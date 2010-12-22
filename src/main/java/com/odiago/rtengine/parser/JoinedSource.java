// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.plan.PlanContext;

/**
 * Represents two sources to a SELECT statement, married by a (windowed) JOIN clause.
 */
public class JoinedSource extends RecordSource {

  private RecordSource mLeftSrc;
  private RecordSource mRightSrc;
  private Expr mJoinExpr;
  private Expr mWindowExpr;

  // All fields exposed by the joined streams.
  private SymbolTable mJoinedSymbols;

  // Symbol representing the key field for the left source.
  private Symbol mLeftKey;

  // Symbol representing the key field for the right source.
  private Symbol mRightKey;

  /**
   * Specifies a statement of the form "... &lt;leftSrc&gt; JOIN &lt;rightSrc&gt; ON ..."
   * @param leftSrc the "primary" source.
   * @param rightSrc the "secondary" source being joined in to the primary.
   * @param joinExpr the expression which must evaluate to true for the join to hold.
   * @param windowExpr an expression that resolves to a window specification, defining
   * the temporal boundaries of the join.
   */
  public JoinedSource(RecordSource leftSrc, RecordSource rightSrc, Expr joinExpr,
      Expr windowExpr) {
    mLeftSrc = leftSrc;
    mRightSrc = rightSrc;
    mJoinExpr = joinExpr;
    mWindowExpr = windowExpr;
  }

  public RecordSource getLeft() {
    return mLeftSrc;
  }

  public RecordSource getRight() {
    return mRightSrc;
  }

  public Expr getJoinExpr() {
    return mJoinExpr;
  }

  public Expr getWindowExpr() {
    return mWindowExpr;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getSourceNames() {
    List<String> out = new ArrayList<String>();
    out.addAll(mLeftSrc.getSourceNames());
    out.addAll(mRightSrc.getSourceNames());
    return out;
  }

  /** {@inheritDoc} */
  @Override
  public SymbolTable getFieldSymbols() {
    return mJoinedSymbols;
  }

  public void setJoinedSymbols(SymbolTable symTab) {
    mJoinedSymbols = symTab;
  }

  public void setLeftKey(Symbol leftKey) {
    mLeftKey = leftKey;
  }

  public Symbol getLeftKey() {
    return mLeftKey;
  }

  public void setRightKey(Symbol rightKey) {
    mRightKey = rightKey;
  }

  public Symbol getRightKey() {
    return mRightKey;
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

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    throw new RuntimeException("joinedsource.createexecplan() needs to be written next.");
    
  }

}
