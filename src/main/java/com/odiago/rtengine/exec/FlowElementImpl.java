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

  private boolean mIsClosed;

  public FlowElementImpl(FlowElementContext ctxt) {
    mContext = ctxt;
    mIsClosed = false;
  }

  /**
   * Emit an event to the next stage in the processing pipeline.
   */
  protected void emit(Event e) throws IOException, InterruptedException {
    mContext.emit(e);
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException, InterruptedException {
    // Default operation: do nothing.
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException, InterruptedException {
    // Notify downstream elements that we're complete.
    mIsClosed = true;
    mContext.notifyCompletion();
  }
  

  /** {@inheritDoc} */
  @Override
  public boolean isClosed() {
    return mIsClosed;
  }

  /** {@inheritDoc} */
  @Override
  public void completeWindow() throws IOException, InterruptedException {
    // Default operation: ignore windowing.
  }
  
  /** {@inheritDoc} */
  @Override
  public FlowElementContext getContext() {
    return mContext;
  }
}
