// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import com.odiago.rtengine.exec.FlowId;

/**
 * Datum payload sent with a LocalEnvironment.ControlOp.Join request.
 */
public class FlowJoinRequest {
  /** What flow to join on. */
  private final FlowId mFlowId;

  /** What object to notify when this flow is done. */
  private final Object mJoinObj;

  public FlowJoinRequest(FlowId flowId, Object joinObj) {
    mFlowId = flowId;
    mJoinObj = joinObj;
  }

  public FlowId getFlowId() {
    return mFlowId;
  }

  public Object getJoinObj() {
    return mJoinObj;
  }
}
