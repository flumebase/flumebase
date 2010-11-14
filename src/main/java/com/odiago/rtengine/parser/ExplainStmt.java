// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.plan.PlanContext;

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

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("EXPLAIN\n");
    mChildStmt.format(sb, depth + 1);
  }

  @Override
  public void createExecPlan(PlanContext planContext) {
    // If we're visiting an EXPLAIN statement, then we don't actually
    // want to execute a flow specification. So we create it via
    // the usual visit sequence, but then we construct a string representation
    // of it, pack that in the message, and discard the actual flow specification
    // to prevent its execution.

    getChildStmt().createExecPlan(planContext);

    StringBuilder sb = planContext.getMsgBuilder();
    sb.append("Parse tree:\n");
    getChildStmt().format(sb, 0);
    sb.append("\n");
    sb.append("Execution plan:\n");
    sb.append(planContext.getFlowSpec().toString());
    sb.append("\n");

    planContext.setFlowSpec(null); // discard flow spec after reifying to string.
  }
}

