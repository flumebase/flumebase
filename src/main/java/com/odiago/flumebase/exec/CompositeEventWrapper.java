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

import java.util.List;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.parser.TypedField;

/**
 * Wraps around a CompositeEvent.
 */
public class CompositeEventWrapper extends EventWrapper {
  private CompositeEvent mEvent;

  public CompositeEventWrapper() {
  }

  @Override
  public void reset(Event e) {
    if (e instanceof CompositeEvent) {
      mEvent = (CompositeEvent) e;
    } else {
      throw new RuntimeException("CompositeEventWrapper.reset() only accepts CompositeEvent");
    }
  }

  public Object getField(TypedField field) throws IOException {
    return mEvent.getField(field);
  }
  
  public Event getEvent() {
    return mEvent;
  }

  @Override
  public String getAttr(String attrName) {
    return mEvent.getAttr(attrName);
  }

  /** Set an attribute on the composite event. */
  public void setAttr(String attrName, String attrVal) {
    mEvent.setAttr(attrName, attrVal);
  }

  @Override
  public String getEventText() {
    List<EventWrapper> innerWrappers = mEvent.getEventWrappers();
    StringBuilder sb = new StringBuilder();
    sb.append("[{");
    boolean first = true;
    for (EventWrapper ew : innerWrappers) {
      if (!first) {
        sb.append("}, {");
      }

      first = false;
      sb.append(ew.getEventText());
    }
    sb.append("}]");
    return sb.toString();
  }
}
