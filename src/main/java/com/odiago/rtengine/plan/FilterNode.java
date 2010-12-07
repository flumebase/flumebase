// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import com.odiago.rtengine.parser.Expr;

/**
 * Filter that passes events through without modification if the filter expression
 * passes.
 */
public class FilterNode extends PlanNode {
  private Expr mFilterExpr;

  public FilterNode(Expr filterExpr) {
    mFilterExpr = filterExpr;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("FilterNode mExpr=(");
    sb.append(mFilterExpr);
    sb.append(")\n");
    formatAttributes(sb);
  }

  public Expr getFilterExpr() {
    return mFilterExpr;
  }
}
