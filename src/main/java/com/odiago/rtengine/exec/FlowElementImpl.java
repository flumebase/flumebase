// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

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

  private int mNumOpenUpstream;

  public FlowElementImpl(FlowElementContext ctxt) {
    mContext = ctxt;
    mIsClosed = false;
    mNumOpenUpstream = 0;
  }

  /** {@inheritDoc} */
  @Override
  public void registerUpstream() {
    mNumOpenUpstream++;
  }

  @Override
  public void closeUpstream() throws IOException, InterruptedException {
    mNumOpenUpstream--;
    if (mNumOpenUpstream == 0) { 
      close();
    }
  }

  /**
   * Emit an event to the next stage in the processing pipeline.
   */
  protected void emit(EventWrapper e) throws IOException, InterruptedException {
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
