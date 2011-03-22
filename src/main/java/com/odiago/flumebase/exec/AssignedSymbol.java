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

package com.odiago.flumebase.exec;

import com.odiago.flumebase.lang.Type;

/**
 * A symbol for a field or expression result within a statement. This symbol
 * includes an assigned internal name which is unique among all the fields
 * and expressions in the total statement.
 *
 * <p>Note that withName() returns a new symbol with the same assigned
 * internal name; they refer to the same field internally, and thus share
 * an internal name.</p>
 */
public class AssignedSymbol extends Symbol {

  // Name assigned to this field during type checking.
  private String mAssignedName;

  // Name of the parent stream that contains this field.
  private String mParentName;

  public AssignedSymbol(String name, Type type, String assignedName) {
    super(name, type);
    mAssignedName = assignedName;
  }

  public String getAssignedName() {
    return mAssignedName;
  }

  public String getParentName() {
    return mParentName;
  }

  public void setParentName(String parentName) {
    mParentName = parentName;
  }

  @Override
  public String toString() {
    return getName() + "[" + mParentName + "." + mAssignedName + "] (" + getType() + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }

    AssignedSymbol assigned = (AssignedSymbol) other;
    return mAssignedName.equals(assigned.mAssignedName)
        && (   (mParentName == null && assigned.mParentName == null)
            || (mParentName != null && mParentName.equals(assigned.mParentName)));
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ mAssignedName.hashCode();
  }

  @Override
  public Symbol withName(String name) {
    AssignedSymbol out = new AssignedSymbol(name, getType(), mAssignedName);
    out.setParentName(mParentName);
    return out;
  }
}
