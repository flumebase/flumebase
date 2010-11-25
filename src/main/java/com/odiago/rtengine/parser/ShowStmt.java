// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.plan.PlanContext;

public class ShowStmt extends SQLStatement {

  /** What to show. */
  private EntityTarget mTarget;

  public ShowStmt(EntityTarget target) {
    mTarget = target;
  }

  public EntityTarget getTarget() {
    return mTarget;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("SHOW " + mTarget + "\n");
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // Just append the requested information in the planContext's
    // StringBuilder. Do not create a flow.

    SymbolTable symTab = planContext.getSymbolTable();
    StringBuilder sb = planContext.getMsgBuilder();

    switch (mTarget) {
    case Stream:
      for (Symbol symbol : symTab) {
        if (symbol.getType().getTypeName().equals(Type.TypeName.STREAM)) {
          // We've found a stream.
          sb.append(symbol.toString());
          sb.append("\n");
        }
      }
      break;
    case Flow:
    default:
      sb.append("Do not know how to show item: " + mTarget);
    }

    PlanContext retContext = new PlanContext(planContext);
    retContext.setFlowSpec(null);
    return retContext;
  }
}
