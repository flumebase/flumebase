/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.server.UserSession;

/**
 * Basic implementation of some key FlowElement methods.
 * FlowElement implementations should subclass this.
 */
public abstract class FlowElementImpl extends FlowElement {
  private static final Logger LOG = LoggerFactory.getLogger(
      FlowElementImpl.class.getName());

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
    if (mNumOpenUpstream == 0 && !isClosed()) { 
      close();
    }
  }

  /**
   * Emit an event to the next stage in the processing pipeline.
   */
  protected void emit(EventWrapper e) throws IOException, InterruptedException {
    emit(e, mContext);
  }


  /**
   * Emit an event to the next stage in the processing pipeline using a
   * specific FlowElementContext.
   */
  protected void emit(EventWrapper e, FlowElementContext context)
      throws IOException, InterruptedException {
    context.emit(e);
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException, InterruptedException {
    // Default operation: do nothing.
    LOG.debug("Opening element class " + getClass().getName());
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException, InterruptedException {
    // Notify downstream elements that we're complete.
    LOG.debug("Closing element class " + getClass().getName());
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
  public FlowElementContext getContext() {
    return mContext;
  }

  /** {@inheritDoc} */
  @Override
  public void onConnect(UserSession session) {
  }
}
