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

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.util.StringUtils;

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
