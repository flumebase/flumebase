// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import com.odiago.flumebase.plan.DropNode;
import com.odiago.flumebase.plan.PlanContext;

/**
 * DROP an object from our set of defined symbols.
 */
public class DropStmt extends SQLStatement {

  /** What type to drop. */
  private EntityTarget mType;

  /** What its name is. */
  private String mName;

  public DropStmt(EntityTarget type, String name) {
    mType = type;
    mName = name;
  }

  public EntityTarget getType() {
    return mType;
  }

  public String getName() {
    return mName;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("DROP mType=" + mType + " mName=" + mName + "\n");
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // Create a DropNode specifying what to drop.
    planContext.getFlowSpec().addRoot(new DropNode(this));
    return planContext;
  }
}
