// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.AbstractQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowElementContext;

/**
 * Parent class of all FlowElementContext implementations which are used
 * within the local environment.
 */
public abstract class LocalContext extends FlowElementContext {
  
  /**
   * The (unbounded) queue where output pending events are placed before delivery.
   */
  private AbstractQueue<PendingEvent> mOutputQueue;

  /** The control operations queue used by the LocalEnvironment. */
  private BlockingQueue<LocalEnvironment.ControlOp> mControlQueue;

  /** Set to true after notifyCompletion() was called once. */
  private boolean mNotifiedCompletion;

  /** Data about the management of the flow by the exec env. */
  private ActiveFlowData mFlowData;

  public LocalContext() {
    mNotifiedCompletion = false;
    mOutputQueue = new ConcurrentLinkedQueue<PendingEvent>();
  }

  /**
   * Post an event to the output queue, to deliver to the specified target
   * FlowElement.
   */
  protected void post(FlowElement target, EventWrapper wrapper) {
    mOutputQueue.add(new PendingEvent(target, wrapper));

    // If the control queue is empty, put a no-op in there to ensure that we fall-thru
    // and process the pending event.
    if (mControlQueue.isEmpty()) {
      mControlQueue.offer(new LocalEnvironment.ControlOp(
          LocalEnvironment.ControlOp.Code.Noop, null));
    }
  }

  /**
   * @return the queue which is used to hold PendingEvents emitted by the
   * context's FlowElement to deliver to other local FlowElements.
   */
  public AbstractQueue<PendingEvent> getPendingEventQueue() {
    return mOutputQueue;
  }

  /**
   * Called by the LocalEnvironment to initialize the control queue instance
   * that is pinged by the post() command in this class.
   */
  void initControlQueue(BlockingQueue<LocalEnvironment.ControlOp> opQueue) {
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
    // TODO(aaron): If the control queue is full, this will deadlock in the same thread.
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

}
