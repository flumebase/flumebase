// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.parser.TypedField;

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
}
