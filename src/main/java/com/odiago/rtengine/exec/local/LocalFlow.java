// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.util.DAG;

/**
 * A DAG of FlowElements to be executed in the local context.
 */
public class LocalFlow extends DAG<FlowElementNode> {
  private FlowId mFlowId;

  public LocalFlow(FlowId id) {
    mFlowId = id;
  }

  public FlowId getId() {
    return mFlowId;
  }


  @Override
  public String toString() {
    return "flow(id=" + mFlowId + ")\n" + super.toString();
  }
}
