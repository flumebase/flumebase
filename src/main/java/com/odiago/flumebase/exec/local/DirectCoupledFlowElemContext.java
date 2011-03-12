// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElement;

/**
 * Context for a FlowElement which has a single downstream FE on the
 * same physical host. An event emitted by the upstream FE is immediately
 * consumed by the downstream FE with no intermediate buffering.
 */
public class DirectCoupledFlowElemContext extends LocalContext {

  /** The downstream element where we sent events. */
  private FlowElement mDownstream;

  public DirectCoupledFlowElemContext(FlowElement downstream) {
    mDownstream = downstream;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void emit(EventWrapper e) throws IOException, InterruptedException {
    mDownstream.takeEvent(e);
  }

  /**
   * Return the downstream FlowElement. Used by the LocalEnvironment.
   */
  @Override
  List<FlowElement> getDownstream() {
    return Collections.singletonList(mDownstream);
  }
}
