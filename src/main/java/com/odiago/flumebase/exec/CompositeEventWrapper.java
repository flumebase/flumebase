// (c) Copyright 2010 Odiago, Inc.

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
