// (c) Copyright 2010 Odiago, Inc.

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
