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
