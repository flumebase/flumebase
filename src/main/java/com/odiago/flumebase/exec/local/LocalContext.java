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

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.FlowElement;
import com.odiago.flumebase.exec.FlowElementContext;

import com.odiago.flumebase.util.concurrent.SelectableQueue;

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
