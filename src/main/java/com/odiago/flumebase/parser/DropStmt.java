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
