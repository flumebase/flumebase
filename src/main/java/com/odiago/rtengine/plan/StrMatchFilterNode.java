// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Filter that passes events through whose complete text matches a specified string.
 */
public class StrMatchFilterNode extends PlanNode {
  private String mMatchStr;

  public StrMatchFilterNode(String match) {
    mMatchStr = match;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("StrMatch text=");
    sb.append(mMatchStr);
  }
}
