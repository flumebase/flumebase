// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

/**
 * Context for a FlowElement that specifies how this FlowElement
 * connects to all its upstream and downstream neighbors.
 */
public abstract class FlowElementContext {

  /**
   * Emit an event to the next downstream FlowElement(s).
   */
  public abstract void emit(EventWrapper e) throws IOException, InterruptedException;

  /**
   * Notify downstream FlowElement(s) that this element will not be
   * providing future events. Downstream FlowElements should themselves
   * emit any final values and complete processing.
   */
  public abstract void notifyCompletion() throws IOException, InterruptedException;
}
