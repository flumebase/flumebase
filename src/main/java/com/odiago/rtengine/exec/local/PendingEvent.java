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
}
