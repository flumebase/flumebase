// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.Iterator;
import java.util.List;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.lang.StreamType;

import com.odiago.rtengine.parser.StreamSourceType;
import com.odiago.rtengine.parser.TypedField;

/**
 * A symbol representing a stream whose input contents are entirely
 * held in memory ahead of time. This is used mostly for testing.
 */
public class InMemStreamSymbol extends StreamSymbol {
  /** The set of events to replay as the contents of the stream. */
  private List<Event> mInputEvents;

  public InMemStreamSymbol(String name, StreamType type, List<Event> inputEvents,
      List<TypedField> fields) {
    super(name, StreamSourceType.Memory, type, null, true, fields);
    mInputEvents = inputEvents;
  }

  public Iterator<Event> getEvents() {
    return mInputEvents.iterator();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append("  records:\n");
    for (Event record : mInputEvents) {
      sb.append("    \"");
      sb.append(new String(record.getBody()));
      sb.append("\"\n");
    }

    return sb.toString();
  }
}
