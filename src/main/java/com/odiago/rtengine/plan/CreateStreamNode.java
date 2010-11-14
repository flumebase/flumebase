// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * DDL operation that creates a stream.
 */
public class CreateStreamNode extends PlanNode {
  private String mStreamName;

  public CreateStreamNode(String streamName) {
    mStreamName = streamName;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("CreateStream name=");
    sb.append(mStreamName);
  }
}
