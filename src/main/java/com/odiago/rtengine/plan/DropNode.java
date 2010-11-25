// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import com.odiago.rtengine.parser.DropStmt;
import com.odiago.rtengine.parser.EntityTarget;

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
