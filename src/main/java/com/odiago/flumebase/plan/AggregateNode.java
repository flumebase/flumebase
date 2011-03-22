/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.plan;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.util.StringUtils;

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
