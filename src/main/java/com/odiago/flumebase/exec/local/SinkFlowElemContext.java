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
import com.odiago.flumebase.exec.FlowId;

/**
 * Context for a FlowElement which is itself a sink; it cannot emit data
 * to any downstream elements, for there are none.
 */
public class SinkFlowElemContext extends LocalContext {

  /** The flow this is operating in. */
  private FlowId mFlow;

  public SinkFlowElemContext(FlowId flow) {
    mFlow = flow;
  }

  @Override
  public void emit(EventWrapper e) throws IOException {
    throw new IOException("Cannot emit event without downstream element");
  }

  FlowId getFlowId() {
    return mFlow;
  }

  @Override
  List<FlowElement> getDownstream() {
    return Collections.emptyList();
  }
}
