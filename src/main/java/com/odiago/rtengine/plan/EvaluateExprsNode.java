// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

public class EvaluateExprsNode extends PlanNode {
  /** Set of expressions that we should calculate for each record. */
  private List<AliasedExpr> mExprs;

  /** Set of fields from our input that we should propagate to our output. */
  private List<TypedField> mPropagateFields;

  public EvaluateExprsNode(List<AliasedExpr> exprs, List<TypedField> propagateFields) {
    mExprs = exprs;
    mPropagateFields = propagateFields;
  }

  public List<AliasedExpr> getExprs() {
    return mExprs;
  }

  public List<TypedField> getPropagateFields() {
    return mPropagateFields;
  }

  @Override
  public void formatParams(StringBuilder sb) {
    sb.append("EvaluateExprs exprs=(");
    StringUtils.formatList(sb, mExprs);
    sb.append("), propagate=(");
    StringUtils.formatList(sb, mPropagateFields);
    sb.append(")\n");
    formatAttributes(sb);
  }
}
