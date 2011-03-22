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

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

/**
 * Abstract class that allows nodes to emit output records which
 * are serialized in avro format.
 */
public abstract class AvroOutputElementImpl extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      AvroOutputElementImpl.class.getName());

  // Avro encoder components reused in our internal workflow.
  private BinaryEncoder mEncoder;
  private GenericDatumWriter<GenericRecord> mDatumWriter;
  private ByteArrayOutputStream mOutputBytes;
  private Schema mOutputSchema;

  public AvroOutputElementImpl(FlowElementContext ctxt, Schema outputSchema) {
    super(ctxt);
    mDatumWriter = new GenericDatumWriter<GenericRecord>(outputSchema);
    mOutputBytes = new ByteArrayOutputStream();
    mEncoder = new BinaryEncoder(mOutputBytes);
    mOutputSchema = outputSchema;
  }

  /**
   * Create a new output Event that encapsulates the specified record,
   * and emit it to the output context.
   * @param record the avro record to emit to the output context.
   * @param inEvent the input event to the current FlowElement; properties
   * of this event are propagated forward into the output event.
   */
  protected void emitAvroRecord(GenericData.Record record, Event inEvent)
      throws IOException, InterruptedException {
    emitAvroRecord(record, inEvent, inEvent.getTimestamp());
  }

  protected void emitAvroRecord(GenericData.Record record, Event inEvent, long timestamp)
      throws IOException, InterruptedException {
    emitAvroRecord(record, inEvent, inEvent.getTimestamp(), getContext());
  }

  protected void emitAvroRecord(GenericData.Record record, Event inEvent, long timestamp,
      FlowElementContext context) throws IOException, InterruptedException {
    // TODO: BAOS.toByteArray() creates a new byte array, as does the
    // creation of the event. That's at least one more array copy than
    // necessary.
    mOutputBytes.reset();
    try {
      mDatumWriter.write(record, mEncoder);
    } catch (NullPointerException npe) {
      // Schema error - the user tried to put a null in a field declared non-null.
      // We silently elide the entire record.
      LOG.debug("Omitting record with NULL value in non-null field: " + npe.toString());
      return;
    }
    Event out = new EventImpl(mOutputBytes.toByteArray(),
        timestamp, inEvent.getPriority(), inEvent.getNanos(), inEvent.getHost()); 
    AvroEventWrapper outWrapper = new AvroEventWrapper(mOutputSchema);
    outWrapper.reset(out);
    emit(outWrapper, context);
  }

  protected Schema getOutputSchema() {
    return mOutputSchema;
  }
}
