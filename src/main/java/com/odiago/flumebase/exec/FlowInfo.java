// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec;

import com.odiago.flumebase.thrift.TFlowInfo;

/**
 * Status information about a flow to report back to the client.
 */
public class FlowInfo {

  /** FlowId of this flow. */
  public final FlowId flowId;

  /** The query that is being executed. */
  public final String query;

  /** Stream name associated with the output of this flow. */
  public final String streamName;

  public FlowInfo(FlowId id, String q, String name) {
    flowId = id;
    query = q;
    streamName = name;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append(flowId.getId());
    sb.append("\t");
    if (null != streamName) {
      sb.append(streamName);
    }
    sb.append("\t");
    sb.append(query);

    return sb.toString();
  }

  public TFlowInfo toThrift() {
    TFlowInfo out = new TFlowInfo();
    out.setFlowId(flowId.toThrift());
    out.setQuery(query);
    out.setStreamName(streamName);
    return out;
  }

  public static FlowInfo fromThrift(TFlowInfo other) {
    return new FlowInfo(FlowId.fromThrift(other.flowId), other.query, other.streamName);
  }

  /** @return the columns associated with our toString() output. */
  public static String getHeader() {
    return "FlowId\tStream\tQuery";
  }
}
