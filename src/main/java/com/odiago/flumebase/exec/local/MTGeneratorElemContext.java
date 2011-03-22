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

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElement;

import com.odiago.flumebase.util.concurrent.ArrayBoundedSelectableQueue;
import com.odiago.flumebase.util.concurrent.SelectableQueue;

/**
 * Context for a FlowElement which has a single downstream FE on the
 * same physical host, but in a differen thread. An event emitted by the
 * upstream FE is pushed into a bounded buffer specific to the downstream
 * FE.
 */
public class MTGeneratorElemContext extends LocalContext {

  /** The downstream element where we sent events. */
  private FlowElement mDownstream;

  private SelectableQueue<Object> mDownstreamQueue;

  public MTGeneratorElemContext(FlowElement downstream) {
    mDownstream = downstream;
  }

  /**
   * Create the downstream queue to communicate with our downstream FlowElement.
   */
  @Override
  public void createDownstreamQueues() {
    mDownstreamQueue = 
        new ArrayBoundedSelectableQueue<Object>(LocalEnvironment.MAX_QUEUE_LEN);
  }

  @Override
  public List<SelectableQueue<Object>> getDownstreamQueues() {
    return Collections.singletonList(mDownstreamQueue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void emit(EventWrapper e) throws IOException, InterruptedException {
    mDownstreamQueue.put(e);
  }

  /**
   * Return the downstream FlowElement. Used by the LocalEnvironment.
   */
  @Override
  List<FlowElement> getDownstream() {
    return Collections.singletonList(mDownstream);
  }
}
