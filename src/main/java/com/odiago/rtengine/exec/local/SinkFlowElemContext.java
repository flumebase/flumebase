// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowId;

/**
 * Context for a FlowElement which is itself a sink; it cannot emit data
 * to any downstream elements, for there are none.
 */
public class SinkFlowElemContext extends LocalContext {

  /** The flow this is operating in. */
  private FlowId mFlow;

  public SinkFlowElemContext(FlowId flow) {
    mFlow = flow;
  }

  @Override
  public void emit(Event e) throws IOException, InterruptedException {
    throw new IOException("Cannot emit event without downstream element");
  }

  FlowId getFlowId() {
    return mFlow;
  }
}
