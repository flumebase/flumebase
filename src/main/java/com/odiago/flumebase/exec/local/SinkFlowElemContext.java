// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElement;
import com.odiago.flumebase.exec.FlowId;

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
  public void emit(EventWrapper e) throws IOException {
    throw new IOException("Cannot emit event without downstream element");
  }

  FlowId getFlowId() {
    return mFlow;
  }

  @Override
  List<FlowElement> getDownstream() {
    return Collections.emptyList();
  }
}
