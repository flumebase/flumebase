// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

/**
 * Context for a FlowElement that specifies how this FlowElement
 * connects to all its upstream and downstream neighbors.
 */
public abstract class FlowElementContext {

  /**
   * Emit an event to the next downstream FlowElement(s).
   */
  public abstract void emit(Event e) throws IOException, InterruptedException;
}
