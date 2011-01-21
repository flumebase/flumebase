// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.testutil;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.core.Event;

import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;

import com.odiago.rtengine.exec.InMemStreamSymbol;

import com.odiago.rtengine.lang.StreamType;
import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.FormatSpec;
import com.odiago.rtengine.parser.TypedField;

/**
 * Builder that creates a StreamSymbol that holds a replayable in-memory stream.
 */
public class MemStreamBuilder {
  private List<Event> mEvents;
  private List<TypedField> mFields;
  private String mStreamName;
  private FormatSpec mFormatSpec;

  public MemStreamBuilder() {
    this(null);
  }

  public MemStreamBuilder(String name) {
    mStreamName = name;
    mEvents = new ArrayList<Event>();
    mFields = new ArrayList<TypedField>();
    mFormatSpec = new FormatSpec();
  }

  public void setName(String name) {
    mStreamName = name;
  }

  public void setFormat(FormatSpec fmt) {
    mFormatSpec = fmt;
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

  public void addField(TypedField tf) {
    mFields.add(tf);
  }

  public void addField(String fieldName, Type fieldType) {
    addField(new TypedField(fieldName, fieldType));
  }

  /**
   * @return a StreamType instance specifying the type of this stream.
   */
  private StreamType makeStreamType() {
    List<Type> colTypes = new ArrayList<Type>();
    for (TypedField field : mFields) {
      colTypes.add(field.getType());
    }

    return new StreamType(colTypes);
  }

  /**
   * @return the InMemStreamSymbol representing this stream.
   */
  public InMemStreamSymbol build() {
    if (null == mStreamName) {
      throw new RuntimeException("Must call setName() to name the stream before building");
    }

    if (0 == mFields.size()) {
      throw new RuntimeException("Must define at least one field with addField()");
    }

    return new InMemStreamSymbol(mStreamName, makeStreamType(), new ArrayList<Event>(mEvents),
        new ArrayList<TypedField>(mFields), mFormatSpec);
  }
}
