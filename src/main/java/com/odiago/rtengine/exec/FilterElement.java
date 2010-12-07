// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.odiago.rtengine.parser.Expr;

/**
 * FlowElement that advances events whose fields when applied to the embedded
 * match expression, cause the match expr to evaluate to true.
 * TODO(aaron): Rewrite this to take expr opcodes, not an ast element.
 */
public class FilterElement extends FlowElementImpl {
  private Expr mFilterExpr;

  public FilterElement(FlowElementContext ctxt, Expr filterExpr) {
    super(ctxt);
    mFilterExpr = filterExpr;
  }


  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    if (Boolean.TRUE.equals(mFilterExpr.eval(e))) {
      emit(e);
    }
  }

  @Override
  public String toString() {
    return "Filter[filterExpr=\"" + mFilterExpr + "\"]";
  }
}
