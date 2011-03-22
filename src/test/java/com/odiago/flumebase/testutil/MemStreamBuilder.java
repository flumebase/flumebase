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

package com.odiago.flumebase.testutil;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.core.Event;

import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.exec.InMemStreamSymbol;
import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.parser.TypedField;

/**
 * Builder that creates a StreamSymbol that holds a replayable in-memory stream.
 */
public class MemStreamBuilder extends StreamBuilder {
  private List<Event> mEvents;
  private InMemStreamSymbol.LatencyPolicy mLatencyPolicy;

  public MemStreamBuilder() {
    this(null);
  }

  public MemStreamBuilder(String name) {
    super(name);
    mEvents = new ArrayList<Event>();
    mLatencyPolicy = InMemStreamSymbol.LatencyPolicy.None;
  }

  public void addEvent(Event e) {
    mEvents.add(e);
  }

  public void addEvent(byte[] eventBody) {
    addEvent(new EventImpl(eventBody));
  }

  public void addEvent(String eventBodyText) {
    addEvent(eventBodyText.getBytes());
  }

  public void addEvent(String eventBodyText, long eventTime) {
    mEvents.add(new EventImpl(eventBodyText.getBytes(), eventTime, Priority.INFO, 0, null));
  }

  public void setLatencyPolicy(InMemStreamSymbol.LatencyPolicy policy) {
    mLatencyPolicy = policy;
  }

  /**
   * @return the InMemStreamSymbol representing this stream.
   */
  @Override
  public StreamSymbol build() {
    if (null == getName()) {
      throw new RuntimeException("Must call setName() to name the stream before building");
    }

    if (0 == getFields().size()) {
      throw new RuntimeException("Must define at least one field with addField()");
    }

    return new InMemStreamSymbol(getName(), makeStreamType(),
        new ArrayList<Event>(mEvents),
        new ArrayList<TypedField>(getFields()), getFormat(),
        mLatencyPolicy);
  }
}
