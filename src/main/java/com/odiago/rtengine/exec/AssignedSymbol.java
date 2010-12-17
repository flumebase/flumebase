// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.lang.Type;

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

  public AssignedSymbol(String name, Type type, String assignedName) {
    super(name, type);
    mAssignedName = assignedName;
  }

  public String getAssignedName() {
    return mAssignedName;
  }

  @Override
  public String toString() {
    return getName() + "[" + mAssignedName + "] (" + getType() + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }

    AssignedSymbol assigned = (AssignedSymbol) other;
    return mAssignedName.equals(assigned.mAssignedName);
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ mAssignedName.hashCode();
  }

  @Override
  public Symbol withName(String name) {
    return new AssignedSymbol(name, getType(), mAssignedName);
  }
}
