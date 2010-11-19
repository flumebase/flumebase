// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.plan.NamedSourceNode;
import com.odiago.rtengine.plan.PlanContext;

/**
 * Specify a source for the FROM clause of a SELECT statement that
 * references the literal name of a stream.
 *
 * A LiteralSource is not an executable SQLStatement, but it shares
 * the common hierarchy.
 */
public class LiteralSource extends SQLStatement {
  private String mSourceName;

  public LiteralSource(String name) {
    mSourceName = name;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("Literal source: ");
    sb.append(mSourceName);
    sb.append("\n");
  }


  public String getName() {
    return mSourceName;
  }


  @Override
  public void createExecPlan(PlanContext planContext) {
    // The execution plan for a literal source is to just open the resouce
    // specified by this abstract source, by looking up its parameters in
    // the symbol table at plan resolution time.

    planContext.getFlowSpec().addRoot(new NamedSourceNode(mSourceName));
  }
}

