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

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElement;

/**
 * A container for an event that should be posted to the FlowElement.
 */
public class PendingEvent {
  private final FlowElement mTarget;
  private final EventWrapper mEvent;

  public PendingEvent(FlowElement fe, EventWrapper event) {
    mTarget = fe;
    mEvent = event;
  }

  public EventWrapper getEvent() {
    return mEvent;
  }

  public FlowElement getFlowElement() {
    return mTarget;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(\"");
    String eventStr = mEvent.getEventText();
    sb.append(eventStr);
    sb.append("\" -> ");
    sb.append(mTarget);
    sb.append(")");
    return sb.toString();
  }
}
