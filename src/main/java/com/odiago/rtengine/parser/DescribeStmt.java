// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.plan.DescribeNode;
import com.odiago.rtengine.plan.PlanContext;

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
  public void createExecPlan(PlanContext planContext) {
    planContext.getFlowSpec().addRoot(new DescribeNode(mIdentifier));
  }
}

