// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.util.DAG;

/**
 * A DAG of FlowElements to be executed in the local context.
 */
public class LocalFlow extends DAG<FlowElementNode> {
  private FlowId mFlowId;

  private boolean mRequiresFlume; 

  public LocalFlow(FlowId id) {
    mFlowId = id;
    mRequiresFlume = false;
  }

  public FlowId getId() {
    return mFlowId;
  }

  /**
   * @return true if Flume is required locally to execute this flow.
   */
  public boolean requiresFlume() {
    return mRequiresFlume;
  }

  /**
   * Called by the LocalFlowBuilder to specify whether this flow requires
   * local Flume elements to be started first.
   */
  void setFlumeRequired(boolean required) {
    mRequiresFlume = required;
  }


  @Override
  public String toString() {
    return "flow(id=" + mFlowId + ")\n" + super.toString();
  }
}
