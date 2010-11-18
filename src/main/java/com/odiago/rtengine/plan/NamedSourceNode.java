// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Input source that reads from a source with a name already defined
 * in the symbol table.
 */
public class NamedSourceNode extends PlanNode {
  private String mStreamName;

  public NamedSourceNode(String streamName) {
    mStreamName = streamName;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("NamedSource streamName=");
    sb.append(mStreamName);
  }

  public String getStreamName() {
    return mStreamName;
  }
}
