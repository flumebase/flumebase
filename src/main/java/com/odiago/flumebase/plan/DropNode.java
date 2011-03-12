// (c) Copyright 2010 Odiago, Inc.

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
