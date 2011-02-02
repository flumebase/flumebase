// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.thrift.TFlowInfo;

/**
 * Status information about a flow to report back to the client.
 */
public class FlowInfo {
  /** FlowId of this flow. */
  public final FlowId flowId;

  /** The query that is being executed. */
  public final String query;

  public FlowInfo(FlowId id, String q) {
    flowId = id;
    query = q;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append(flowId.getId());
    sb.append("\t");
    sb.append(query);

    return sb.toString();
  }

  public TFlowInfo toThrift() {
    return new TFlowInfo(flowId.toThrift(), query);
  }
  
  public static FlowInfo fromThrift(TFlowInfo other) {
    return new FlowInfo(FlowId.fromThrift(other.flowId), other.query);
  }
}
