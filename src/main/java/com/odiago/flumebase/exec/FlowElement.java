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

import com.odiago.flumebase.server.UserSession;

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
  
  /** Event attribute name defining the origin stream of an event. */
  public static final String STREAM_NAME_ATTR = "rtsql:stream";

  /**
   * Called before the FlowElement processes any events. This may trigger
   * the FlowElement to begin emitting generated events, etc.
   */
  public abstract void open() throws IOException, InterruptedException;

  /** Notify to the FlowElement that it has an open upstream element. */
  public abstract void registerUpstream();

  /**
   * Notify an element that one of its upstream elements has closed. When
   * the last upstream element notifies this one of its closure, this will
   * close the current FlowElement.
   */
  public abstract void closeUpstream() throws IOException, InterruptedException;

  /**
   * Called to notify the FlowElement to stop processing events. After
   * the close() call returns, the FE may not emit further events
   * downstream. After this call returns, isClosed() should return true.
   */
  public abstract void close() throws IOException, InterruptedException;

  /**
   * Returns true if close() has already been called on this FlowElement.
   */
  public abstract boolean isClosed();

  /**
   * Process another input event in the current window.
   * May emit output events if there is a 1-to-1 (non-aggregate) correlation.
   */
  public abstract void takeEvent(EventWrapper e) throws IOException, InterruptedException;

  /**
   * @return the FlowElementContext that the element is bound to.
   */
  public abstract FlowElementContext getContext();
  
  /**
   * Perform any action which needs to happen when a user subscribes to this flow.
   * Note that this occurs just before the subscription; the session is not yet
   * actually subscribed to the flow.
   */
  public abstract void onConnect(UserSession session);
}
