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
