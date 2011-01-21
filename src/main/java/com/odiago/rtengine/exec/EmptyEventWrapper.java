// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.parser.TypedField;

/**
 * An EventWrapper instance that wraps no actual data; it returns null
 * for all fields. It is intended to be passed to Expr.eval() methods
 * which are guaranteed to be constant, and should not depend on any
 * per-record state. The getField() method throws IOException for all
 * requests.
 */
public class EmptyEventWrapper extends EventWrapper {
  private Event mEvent;

  public EmptyEventWrapper() {
  }

  @Override
  public void reset(Event e) {
    mEvent = e;
  }

  @Override
  public Object getField(TypedField field) throws IOException {
    throw new IOException("EmptyEventWrapper cannot access field: " + field);
  }

  @Override
  public Event getEvent() {
    return mEvent;
  }

  @Override
  public String getAttr(String attrName) {
    return null;
  }

  @Override
  public String getEventText() {
    return "(empty)";
  }
}
