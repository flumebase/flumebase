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

package com.odiago.flumebase.plan;

import com.odiago.flumebase.parser.DropStmt;
import com.odiago.flumebase.parser.EntityTarget;

/**
 * Drop an object from the database.
 */
public class DropNode extends PlanNode {
  private EntityTarget mType;
  private String mName;

  public DropNode(DropStmt dropStmt) {
    mType = dropStmt.getType();
    mName = dropStmt.getName();
  }

  public EntityTarget getType() {
    return mType; // FLOW, STREAM, etc...
  }

  public String getName() {
    return mName;
  }
}
