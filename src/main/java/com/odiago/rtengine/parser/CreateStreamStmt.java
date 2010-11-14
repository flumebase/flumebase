// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.plan.CreateStreamNode;
import com.odiago.rtengine.plan.PlanContext;

/**
 * CREATE STREAM statement.
 */
public class CreateStreamStmt extends SQLStatement {
  private String mName;

  public CreateStreamStmt(String streamName) {
    mName = streamName;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("CREATE STREAM mName=");
    sb.append(mName);
    sb.append("\n");
  }

  @Override
  public void createExecPlan(PlanContext planContext) {
    // The execution plan for a CREATE STREAM statement is to
    // perform the DDL operation by itself and quit.

    String streamName = unquote(mName);
    planContext.getFlowSpec().addRoot(new CreateStreamNode(streamName));
  }
}

