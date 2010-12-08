// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

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
        inEvent.getTimestamp(), inEvent.getPriority(), inEvent.getNanos(), inEvent.getHost()); 
    AvroEventWrapper outWrapper = new AvroEventWrapper(mOutputSchema);
    outWrapper.reset(out);
    emit(outWrapper);
  }

  protected Schema getOutputSchema() {
    return mOutputSchema;
  }
}
