// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

import com.odiago.rtengine.parser.TypedField;

/**
 * Transform each binary-encoded Avro event we receive, projecting from its
 * input schema to our (narrower) output schema.
 */
public class ProjectionElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      ProjectionElement.class.getName());
  // Avro encoder components reused in our internal workflow.
  private BinaryEncoder mEncoder;
  private GenericDatumWriter<GenericRecord> mDatumWriter;
  private ByteArrayOutputStream mOutputBytes;
  private Schema mOutputSchema;

  private List<TypedField> mOutputFields;

  public ProjectionElement(FlowElementContext ctxt, Schema outputSchema,
      List<TypedField> outputFields) {
    super(ctxt);
    mDatumWriter = new GenericDatumWriter<GenericRecord>(outputSchema);
    mOutputBytes = new ByteArrayOutputStream();
    mEncoder = new BinaryEncoder(mOutputBytes);
    mOutputSchema = outputSchema;
    mOutputFields = new ArrayList<TypedField>(outputFields);
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    GenericData.Record record = new GenericData.Record(mOutputSchema);

    for (TypedField field : mOutputFields) {
      String fieldName = field.getName();
      record.put(fieldName, e.getField(field));
    }

    LOG.info("Projection emits: " + record);

    // TODO: BAOS.toByteArray() creates a new byte array, as does the
    // creation of the event. That's at least one more array copy than
    // necessary.
    mOutputBytes.reset();
    mDatumWriter.write(record, mEncoder);
    Event inEvent = e.getEvent();
    Event out = new EventImpl(mOutputBytes.toByteArray(),
        inEvent.getTimestamp(), inEvent.getPriority(), inEvent.getNanos(), inEvent.getHost()); 
    AvroEventWrapper outWrapper = new AvroEventWrapper(mOutputSchema);
    outWrapper.reset(out);
    emit(outWrapper);
  }
}
