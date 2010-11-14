// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

/**
 * Context for a FlowElement which has a single downstream FE on the
 * same physical host. An event emitted by the upstream FE is immediately
 * consumed by the downstream FE with no intermediate buffering.
 */
public class DirectCoupledFlowElemContext extends FlowElementContext {

  /** The downstream element where we sent events. */
  private FlowElement mDownstream;

  public DirectCoupledFlowElemContext(FlowElement downstream) {
    mDownstream = downstream;
  }

  @Override
  public void emit(Event e) throws IOException, InterruptedException {
    mDownstream.takeEvent(e);
  }
}
