// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowElementContext;

import com.odiago.rtengine.util.concurrent.SelectableQueue;

/**
 * Parent class of all FlowElementContext implementations which are used
 * within the local environment.
 */
public abstract class LocalContext extends FlowElementContext {
  
  /** The control operations queue used by the LocalEnvironment. */
  private SelectableQueue<Object> mControlQueue;

  /** Set to true after notifyCompletion() was called once. */
  private boolean mNotifiedCompletion;

  /** Data about the management of the flow by the exec env. */
  private ActiveFlowData mFlowData;

  public LocalContext() {
    mNotifiedCompletion = false;
  }

  /**
   * Called by the LocalEnvironment to initialize the control queue instance
   * that is pinged by the post() command in this class.
   */
  void initControlQueue(SelectableQueue<Object> opQueue) {
    mControlQueue = opQueue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void notifyCompletion() throws IOException, InterruptedException {
    if (mNotifiedCompletion) {
      return; // Already did this.
    }

    // Specify to the LocalEnvironment that we are done processing. 
    mControlQueue.put(new LocalEnvironment.ControlOp(
        LocalEnvironment.ControlOp.Code.ElementComplete,
        new LocalCompletionEvent(this)));
    mNotifiedCompletion = true;
  }

  void setFlowData(ActiveFlowData flowData) {
    mFlowData = flowData;
  }

  public ActiveFlowData getFlowData() {
    return mFlowData;
  }

  /**
   * Create any necessary downstream communication queues. Default: don't create any queues.
   */
  public void createDownstreamQueues() {
  }

  /**
   * @return a list of all downstream communication queues. If this list is non-empty,
   * then order matters.  The elements in this list correspond to entries in
   * getDownstream(). The output list may contain null values.
   */
  public List<SelectableQueue<Object>> getDownstreamQueues() {
    return Collections.emptyList();
  }

  /** @return a list of all downstream FlowElements. */
  abstract List<FlowElement> getDownstream();
}
