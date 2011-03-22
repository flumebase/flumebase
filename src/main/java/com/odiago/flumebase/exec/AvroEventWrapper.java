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

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.parser.TypedField;

/**
 * An EventWrapper that operates on events containing parsed records
 * that are encoded as Avro record instances.
 */
public class AvroEventWrapper extends EventWrapperImpl {
  private Event mEvent;
  private boolean mIsDecoded; // true if mEvent has been decoded into mRecord.

  // Members used to decode Avro into fields.
  private DecoderFactory mDecoderFactory;
  private BinaryDecoder mDecoder;
  private GenericData.Record mRecord;
  private GenericDatumReader<GenericData.Record> mGenericReader;

  public AvroEventWrapper(Schema inputSchema) {
    this(inputSchema, inputSchema);
  }

  public AvroEventWrapper(Schema inputSchema, Schema outputSchema) {
    mDecoderFactory = new DecoderFactory();
    mRecord = new GenericData.Record(inputSchema);
    mGenericReader = new GenericDatumReader<GenericData.Record>(inputSchema, outputSchema);
  }

  @Override
  public void reset(Event e) {
    mEvent = e;
    mIsDecoded = false;
  }

  /**
   * Decode mEevent into mRecord.
   */
  private void decode() throws IOException {
    mDecoder = mDecoderFactory.createBinaryDecoder(mEvent.getBody(), mDecoder);
    mRecord = mGenericReader.read(mRecord, mDecoder);
    mIsDecoded = true;
  }

  @Override
  public Object getField(TypedField field) throws IOException {
    if (!mIsDecoded) {
      // TODO(aaron): Figure out how to decode more lazily; if we knew which subset
      // of fields we might access, we could project onto a narrower reader schema
      // and only decode those fields...
      decode();
    }

    return mRecord.get(field.getAvroName());
  }

  @Override
  public Event getEvent() {
    return mEvent;
  }

  /**
   * @return the parsed avro record.
   */
  public GenericData.Record getRecord() throws IOException {
    if (!mIsDecoded) {
      decode();
    }
    return mRecord;
  }
}
