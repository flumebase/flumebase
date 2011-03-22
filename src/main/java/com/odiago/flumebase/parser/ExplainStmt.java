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

package com.odiago.flumebase.parser;

import com.odiago.flumebase.plan.PlanContext;

/**
 * EXPLAIN statement.
 */
public class ExplainStmt extends SQLStatement {
  private SQLStatement mChildStmt;

  public ExplainStmt(SQLStatement child) {
    mChildStmt = child;
  }

  public SQLStatement getChildStmt() {
    return mChildStmt;
  }

  public void setChildStmt(SQLStatement child) {
    mChildStmt = child;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("EXPLAIN\n");
    mChildStmt.format(sb, depth + 1);
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // If we're visiting an EXPLAIN statement, then we don't actually
    // want to execute a flow specification. So we create it via
    // the usual visit sequence, but then we set a flag telling the caller
    // to construct a string representation of it rather than execute it.

    getChildStmt().createExecPlan(planContext);

    StringBuilder sb = planContext.getMsgBuilder();
    sb.append("Parse tree:\n");
    getChildStmt().format(sb, 0);
    sb.append("\n");

    PlanContext retContext = new PlanContext(planContext);
    retContext.setExplain(true);
    return retContext;
  }
}

