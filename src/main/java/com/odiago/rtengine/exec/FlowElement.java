// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

/**
 * A flow element is a worker in a flow; it has an input side
 * and an output side, and some function. 
 * Flow elements perform filtering, aggregation, joins, etc.
 * Flow elements represent specific physical entities.
 * Multiple flow elements may perform the same function. For example,
 * the same filter ("forward all messages matching regex 'r'") may
 * be applied to multiple input streams in parallel. Each such input
 * stream requires a separate FlowElement to be deployed.
 */
public abstract class FlowElement {

  /**
   * Called before the FlowElement processes any events. This may trigger
   * the FlowElement to begin emitting generated events, etc.
   */
  public abstract void open() throws IOException, InterruptedException;

  /**
   * Called to notify the FlowElement to stop processing events. After
   * the close() call returns, the FE may not emit further events
   * downstream.
   */
  public abstract void close() throws IOException, InterruptedException;

  /**
   * Process another input event in the current window.
   * May emit output events if there is a 1-to-1 (non-aggregate) correlation.
   */
  public abstract void takeEvent(Event e) throws IOException, InterruptedException;

  /**
   * Finish any aggregation associated with this window, and emit any output
   * events that are associated with the entire window.
   */
  public void completeWindow() throws IOException, InterruptedException {
    // Default operation: ignore windowing.
  }
  
}
