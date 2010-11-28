// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowElement;

/**
 * A container for an event that should be posted to the FlowElement.
 */
public class PendingEvent {
  private final FlowElement mTarget;
  private final Event mEvent;

  public PendingEvent(FlowElement fe, Event event) {
    mTarget = fe;
    mEvent = event;
  }

  public Event getEvent() {
    return mEvent;
  }

  public FlowElement getFlowElement() {
    return mTarget;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(\"");
    String eventStr = new String(mEvent.getBody());
    sb.append(eventStr);
    sb.append("\" -> ");
    sb.append(mTarget);
    sb.append(")");
    return sb.toString();
  }
}
