// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Informational operation: describe an object (stream, etc).
 */
public class DescribeNode extends PlanNode {
  private String mIdentifier;

  public DescribeNode(String identifier) {
    mIdentifier = identifier;
  }

  public String getIdentifier() {
    return mIdentifier;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("Describe id=");
    sb.append(mIdentifier);
  }
}
