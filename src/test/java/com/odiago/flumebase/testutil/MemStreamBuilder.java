// (c) Copyright 2010 Odiago, Inc.

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
