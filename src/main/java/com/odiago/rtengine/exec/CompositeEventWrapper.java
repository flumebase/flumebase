// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.parser.TypedField;

/**
 * Wraps around multiple underlying EventWrappers, to join
 * their results together.
 */
public class CompositeEventWrapper extends EventWrapper {
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

  public CompositeEventWrapper(Map<String, Integer> fieldBindings) {
    mEventWrappers = new ArrayList<EventWrapper>();
    mFieldBindings = fieldBindings;
    mAttrs = new TreeMap<String, String>();
  }

  @Override
  public void reset(Event e) {
    throw new RuntimeException("CompositeEventWrapper cannot be reset");
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

  @Override
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
}
