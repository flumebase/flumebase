// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * Execution stage where aggregation takes place.
 */
public class AggregateNode extends PlanNode {

  private final Configuration mConf;

  // Set of typed fields to aggregate by. may be empty.
  private final List<TypedField> mGroupByFields; 

  // Expression specifying the aggregation window bounds.
  private final Expr mWindowExpr; 

  // Aggregate fns to evaluate. Each of these is just a FnCallExpr in an AliasedExpr.
  private final List<AliasedExpr> mAggregateExprs;

  // List of fields whose values must be propagated forward by this execution layer.
  private final List<TypedField> mPropagateFields;

  public AggregateNode(List<TypedField> groupByFields, Expr windowExpr,
      List<AliasedExpr> aggregateExprs, List<TypedField> propagateFields,
      Configuration conf) {
    mGroupByFields = groupByFields;
    mWindowExpr = windowExpr;
    mAggregateExprs = aggregateExprs;
    mPropagateFields = propagateFields;
    mConf = conf;

    // Aggregate node will need an eviction timer.
    this.setAttr(PlanNode.USES_TIMER_ATTR, Boolean.TRUE);
  }

  public List<TypedField> getGroupByFields() {
    return mGroupByFields;
  }

  public Expr getWindowExpr() {
    return mWindowExpr;
  }

  public List<AliasedExpr> getAggregateExprs() {
    return mAggregateExprs;
  }

  public List<TypedField> getPropagateFields() {
    return mPropagateFields;
  }

  public Configuration getConf() {
    return mConf;
  }

  @Override
  public void formatParams(StringBuilder sb) {
    sb.append("Aggregate groupBy=[");
    StringUtils.formatList(sb, mGroupByFields);
    sb.append("], over window=(");
    sb.append(mWindowExpr.toStringOneLine());
    sb.append(") for exprs=[");
    boolean first = true;
    for (AliasedExpr ae : mAggregateExprs) {
      if (!first) {
        sb.append(", ");
      }

      ae.format(sb);
      first = false;
    }
    sb.append("], propagate=[");
    StringUtils.formatList(sb, mPropagateFields);
    sb.append("]\n");
    formatAttributes(sb);
  }
}
