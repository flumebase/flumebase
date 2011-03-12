// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import com.odiago.flumebase.plan.DescribeNode;
import com.odiago.flumebase.plan.PlanContext;

/**
 * DESCRIBE statement.
 */
public class DescribeStmt extends SQLStatement {
  /** The object to describe. */
  private String mIdentifier;

  public DescribeStmt(String identifier) {
    mIdentifier = identifier;
  }

  public String getIdentifier() {
    return mIdentifier;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("DESCRIBE mId=[" + mIdentifier + "]\n");
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    planContext.getFlowSpec().addRoot(new DescribeNode(mIdentifier));
    return planContext;
  }
}

