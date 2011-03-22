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

package com.odiago.flumebase.io;

import java.io.IOException;

import java.util.List;
import java.util.Map;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.TypedField;

public class AvroEventParser extends EventParser {
  private static final Logger LOG = LoggerFactory.getLogger(
      AvroEventParser.class.getName());

  public static final String SCHEMA_PARAM = "schema";

  /** Configuration parameters. */
  private Map<String, String> mParams;

  /** Current event we're parsing. */
  private Event mEvent;

  /** Schema for input events */
  private Schema mSchema;

  /** Current event deserialized into a generic data record */
  private GenericData.Record mRecord;

  private boolean mIsDecoded; // true if the mEvent is deserialized into mRecord.

  // Avro parsing utility objects below.

  private DecoderFactory mDecoderFactory;
  private BinaryDecoder mDecoder;
  private GenericDatumReader<GenericData.Record> mDatumReader;

  /**
   * Creates a new AvroEventParser, with a string--string parameter map specified
   * by the user who created the stream we are parsing.
   */
  public AvroEventParser(Map<String, String> params) {
    mParams = params;

    // Initialize avro decoder.
    String schemaStr = mParams.get(SCHEMA_PARAM);
    if (null != schemaStr) {
      // If schemaStr is null, validate() will fail, so we won't
      // need these things that we can't initialize.
      try {
        mSchema = Schema.parse(schemaStr);
        mDecoderFactory = new DecoderFactory();
        mRecord = new GenericData.Record(mSchema);
        mDatumReader = new GenericDatumReader<GenericData.Record>(mSchema);
      } catch (RuntimeException re) {
        // Couldn't parse schema. Ok, we'll get this in the validate() method.
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void reset(Event e) {
    mEvent = e;
    mIsDecoded = false;
  }

  /** {@inheritDoc} */
  @Override
  public Object getColumn(int colIdx, Type expectedType)
      throws IOException {

    if (!mIsDecoded) {
      // Now that we actually want a record value, decode the input bytes.
      mDecoder = mDecoderFactory.createBinaryDecoder(mEvent.getBody(), mDecoder);
      mRecord = mDatumReader.read(mRecord, mDecoder);
      mIsDecoded = true;
    }
    return mRecord.get(colIdx);
  }

  @Override
  public boolean validate(StreamSymbol streamSym) {
    // Check that we have an incoming schema.
    String schemaStr = mParams.get(SCHEMA_PARAM);
    if (null == schemaStr) {
      LOG.error("The EventParser for this stream requires the '"
          + SCHEMA_PARAM + "' property to be set. Try recreating the stream as: "
          + "CREATE STREAM .. EVENT FORMAT 'avro' PROPERTIES ('" + SCHEMA_PARAM
          + "' = ...)");
      return false;
    } else {
      try {
        Schema.parse(schemaStr);
      } catch (RuntimeException re) {
        LOG.error("Couldn't parse specified schema for the stream: " + re);
        return false;
      }

      // Given a schema -- does it match the expected column types?
      // TODO -- note that we can induce the field defs from the schema..
      // we should be able to say something like: CREATE STREAM foo (auto) FROM SCHEMA '....'
      List<Schema.Field> schemaFields = null;
      try {
        schemaFields = mSchema.getFields();
      } catch (AvroRuntimeException are) {
        // This wasn't a record schema, it was a single field or something.
        LOG.error("Schemas for events must be of record type.");
        return false;
      }
      List<TypedField> columnFields = streamSym.getFields();

      if (schemaFields.size() != columnFields.size()) {
        LOG.error("The schema specified for this stream has a different number "
            + "of fields than are specified in the stream definition.");
        return false;
      }

      for (int i = 0; i < schemaFields.size(); i++) {
        TypedField col = columnFields.get(i);
        Type colType = col.getType();
        Schema.Field schemaField = schemaFields.get(i);

        if (!schemaField.name().equals(col.getUserAlias())) {
          // TODO -- does this matter? We address these fields by index anyway, not by name..
          // Just warn them.
          LOG.warn("Column " + col.getUserAlias() + " is aligned with an Avro field "
              + "with the name: " + schemaField.name());
        }
        
        // More important: are the schemas compatible?
        if (!schemaField.schema().equals(colType.getAvroSchema())) {
          LOG.error("Column " + col.getUserAlias() + " has type " + colType + " but has "
              + " an incompatible Avro schema: " + schemaField.schema());
          return false;
        }
      }
    }

    // Looks good!
    return true;
  }


}
