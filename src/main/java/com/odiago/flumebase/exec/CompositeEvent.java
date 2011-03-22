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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.parser.TypedField;

/**
 * Wraps around multiple underlying EventWrappers, to join
 * their results together.
 */
public class CompositeEvent extends Event {
  /**
   * An ordered list of event wrappers that we are combining into one
   * logical event.
   */
  private List<EventWrapper> mEventWrappers;

  /**
   * A mapping from field name to indexes in mEventWrappers; determines
   * which mEventWrappers[i] should satisfy a given getField() call.
   */
  private Map<String, Integer> mFieldBindings;

  /**
   * Allow specification of attributes for the total "event" we
   * are wrapping.
   */
  private Map<String, String> mAttrs;

  // fields for Event implementation.

  private Event.Priority mPriority;
  private long mTimestamp;
  private long mNanos;
  private String mHost;

  public CompositeEvent(Map<String, Integer> fieldBindings, Event.Priority priority,
      long timestamp, long nanos, String host) {
    mEventWrappers = new ArrayList<EventWrapper>();
    mFieldBindings = fieldBindings;
    mAttrs = new TreeMap<String, String>();

    mPriority = priority;
    mTimestamp = timestamp;
    mNanos = nanos;
    mHost = host;
  }

  public Object getField(TypedField field) throws IOException {
    Integer index = mFieldBindings.get(field.getAvroName());
    assert null != index;
    int idx = index.intValue();
    assert idx < mEventWrappers.size() && idx >= 0;
    return mEventWrappers.get(idx).getField(field);
  }
  
  public Event getEvent() {
    // Don't know which raw event is appropriate here.
    return null;
  }

  public String getAttr(String attrName) {
    // First, check our own attribute map.
    String val = mAttrs.get(attrName);

    // If that's null, try all the wrapped events, in order, to see
    // if it's set by one of them.
    for (int i = 0; i < mEventWrappers.size() && val == null; i++) {
      val = mEventWrappers.get(i).getAttr(attrName);
    }

    return val;
  }

  /** Set an attribute on the composite event. */
  public void setAttr(String attrName, String attrVal) {
    mAttrs.put(attrName, attrVal);
  }

  /**
   * Add an EventWrapper to the set of events we composite over.
   */
  public void add(EventWrapper eventWrapper) {
    mEventWrappers.add(eventWrapper);
  }


  public byte[] getBody() {
    return null;
  }

  public Event.Priority getPriority() {
    return mPriority;
  }

  public long getTimestamp() {
    return mTimestamp;
  }

  public long getNanos() {
    return mNanos;
  }

  public String getHost() {
    return mHost;
  }

  public byte[] get(String attrName) {
    // First, check our own attribute map.
    String val = mAttrs.get(attrName);

    // If that's null, try all the wrapped events, in order, to see
    // if it's set by one of them.
    for (int i = 0; i < mEventWrappers.size() && val == null; i++) {
      val = mEventWrappers.get(i).getAttr(attrName);
    }

    if (null == val) {
      return null;
    } else {
      return val.getBytes();
    }
  }

  public void set(String attrName, byte[] val) {
    setAttr(attrName, new String(val));
  }

  public Map<String, byte[]> getAttrs() {
    return null;
  }

  public void merge(Event arg) {
    throw new RuntimeException("Unsupported operation.");
  }

  public void hierarchicalMerge(String arg, Event event) {
    throw new RuntimeException("Unsupported operation.");
  }

  /** @return the set of EventWrappers that compose this composite event. */
  List<EventWrapper> getEventWrappers() {
    return mEventWrappers;
  }
}
