// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.util.AbstractQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.cloudera.flume.core.Event;

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

  public LocalContext() {
    mOutputQueue = new ConcurrentLinkedQueue<PendingEvent>();
  }

  /**
   * Post an event to the output queue, to deliver to the specified target
   * FlowElement.
   */
  protected void post(FlowElement target, Event event) {
    mOutputQueue.add(new PendingEvent(target, event));

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

}
