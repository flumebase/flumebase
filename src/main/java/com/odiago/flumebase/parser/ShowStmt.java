// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.plan.PlanContext;

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

  /**
   * Formats all entries of symTab with the specified symbolType into
   * strings appended to the stringbuilder argument.
   */
  private void showAllSymbols(StringBuilder sb, SymbolTable symTab, Type.TypeName symbolType) {
    for (Symbol symbol : symTab) {
      if (symbol.getType().getTypeName().equals(symbolType)) {
        // We've found one
        sb.append(symbol.toString());
        sb.append("\n");
      }
    }
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // Just append the requested information in the planContext's
    // StringBuilder. Do not create a flow.

    SymbolTable symTab = planContext.getSymbolTable();
    StringBuilder sb = planContext.getMsgBuilder();

    switch (mTarget) {
    case Stream:
      showAllSymbols(sb, symTab, Type.TypeName.STREAM);
      break;
    case Function:
      showAllSymbols(sb, symTab, Type.TypeName.SCALARFUNC);
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
