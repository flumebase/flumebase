// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.ParsingEventWrapper;

import com.odiago.rtengine.io.ColumnParseException;
import com.odiago.rtengine.io.EventParser;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.TypedField;

/**
 * An EventWrapper that operates on raw input events and parses them
 * into fields.
 */
public class ParsingEventWrapper extends EventWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(
      ParsingEventWrapper.class.getName());
  private EventParser mParser;
  private Event mEvent;

  /** An ordered list of field names. */
  private List<String> mFieldNames;

  public ParsingEventWrapper(EventParser parser, List<String> fieldNames) {
    mParser = parser;
    mFieldNames = new ArrayList<String>(fieldNames);
  }

  @Override
  public void reset(Event e) {
    mEvent = e;
    mParser.reset(e);
  }

  @Override
  public Object getField(TypedField field) throws IOException {
    int pos = mFieldNames.indexOf(field.getName());
    return getField(pos, field.getType());
  }

  /**
   * Perform the field lookup without translating a field name into an index;
   * reference the field by index directly, as the EventParser does.
   */
  public Object getField(int fieldIdx, Type fieldType) {
    try {
      return mParser.getColumn(fieldIdx, fieldType);
    } catch (ColumnParseException cpe) {
      // We couldn't parse the field - it may not exist in the input,
      // or it may not be parsible according to the field's type.
      if (fieldType.isNullable()) {
        // Store a null in this field. This is ok.
        return null;
      } else {
        // Discard this input record; it cannot be parsed.
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not parse field " + fieldIdx + ": " + cpe);
        }
        return null;
      }
    }
  }

  @Override
  public Event getEvent() {
    return mEvent;
  }
}
