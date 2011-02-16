// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElement;

import com.odiago.rtengine.util.concurrent.ArrayBoundedSelectableQueue;
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

  /**
   * Create the downstream queue to communicate with our downstream FlowElement.
   */
  @Override
  public void createDownstreamQueues() {
    mDownstreamQueue = 
        new ArrayBoundedSelectableQueue<Object>(LocalEnvironment.MAX_QUEUE_LEN);
  }

  @Override
  public List<SelectableQueue<Object>> getDownstreamQueues() {
    return Collections.singletonList(mDownstreamQueue);
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
  @Override
  List<FlowElement> getDownstream() {
    return Collections.singletonList(mDownstream);
  }
}
