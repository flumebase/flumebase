/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.ParsingEventWrapper;

import com.odiago.flumebase.io.ColumnParseException;
import com.odiago.flumebase.io.EventParser;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.TypedField;

/**
 * An EventWrapper that operates on raw input events and parses them
 * into fields.
 */
public class ParsingEventWrapper extends EventWrapperImpl {
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
    int pos = mFieldNames.indexOf(field.getAvroName());
    // The field name should have an index, or else there was an error during
    // the logical planning phase.
    assert pos != -1;
    return getField(pos, field.getType());
  }

  /**
   * Perform the field lookup without translating a field name into an index;
   * reference the field by index directly, as the EventParser does.
   */
  public Object getField(int fieldIdx, Type fieldType) throws IOException {
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
