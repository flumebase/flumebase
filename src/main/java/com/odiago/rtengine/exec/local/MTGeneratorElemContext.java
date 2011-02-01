// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElement;

import com.odiago.rtengine.util.concurrent.SelectableQueue;

/**
 * Context for a FlowElement which has a single downstream FE on the
 * same physical host, but in a differen thread. An event emitted by the
 * upstream FE is pushed into a bounded buffer specific to the downstream
 * FE.
 */
public class MTGeneratorElemContext extends LocalContext {

  /** The downstream element where we sent events. */
  private FlowElement mDownstream;

  private SelectableQueue<Object> mDownstreamQueue;

  public MTGeneratorElemContext(FlowElement downstream) {
    mDownstream = downstream;
  }

  /** Specify which queue to populate with values for the downstream FlowElement. */
  public void setDownstreamQueue(SelectableQueue<Object> queue) {
    mDownstreamQueue = queue;
  }

  public SelectableQueue<Object> getDownstreamQueue() {
    return mDownstreamQueue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void emit(EventWrapper e) throws IOException, InterruptedException {
    mDownstreamQueue.put(e);
  }

  /**
   * Return the downstream FlowElement. Used by the LocalEnvironment.
   */
  FlowElement getDownstream() {
    return mDownstream;
  }
}
