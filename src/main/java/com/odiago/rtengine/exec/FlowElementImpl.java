// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

/**
 * Basic implementation of some key FlowElement methods.
 * FlowElement implementations should subclass this.
 */
public abstract class FlowElementImpl extends FlowElement {

  /** The context object that specifies how this FE connects to the next
   * one, etc.
   */
  private FlowElementContext mContext;

  public FlowElementImpl(FlowElementContext ctxt) {
    mContext = ctxt;
  }

  /**
   * Emit an event to the next stage in the processing pipeline.
   */
  protected void emit(Event e) throws IOException, InterruptedException {
    mContext.emit(e);
  }

  /** {@inheritDoc} */
  public void open() throws IOException, InterruptedException {
    // Default operation: do nothing.
  }

  /** {@inheritDoc} */
  public void close() throws IOException, InterruptedException {
  }

  /** {@inheritDoc} */
  public void completeWindow() throws IOException, InterruptedException {
    // Default operation: ignore windowing.
  }
  
}
