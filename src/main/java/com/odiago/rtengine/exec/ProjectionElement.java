// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

/**
 * Transform each binary-encoded Avro event we receive, projecting from its
 * input schema to our (narrower) output schema.
 */
public class ProjectionElement extends FlowElementImpl {

  // Avro encoder/decoder components reused in our internal workflow.
  private DecoderFactory mDecoderFactory;
  private BinaryDecoder mDecoder;
  private BinaryEncoder mEncoder;
  private GenericDatumReader<GenericRecord> mDatumReader;
  private GenericDatumWriter<GenericRecord> mDatumWriter;
  private GenericRecord mRecord;
  private ByteArrayOutputStream mOutputBytes;

  public ProjectionElement(FlowElementContext ctxt, Schema inputSchema, Schema outputSchema) {
    super(ctxt);
    mDecoderFactory = new DecoderFactory();
    mDatumReader = new GenericDatumReader<GenericRecord>(inputSchema, outputSchema);
    mDatumWriter = new GenericDatumWriter<GenericRecord>(outputSchema);
    mOutputBytes = new ByteArrayOutputStream();
    mEncoder = new BinaryEncoder(mOutputBytes);
  }

  @Override
  public void takeEvent(Event e) throws IOException, InterruptedException {
    byte[] body = e.getBody();
    mDecoder = mDecoderFactory.createBinaryDecoder(body, mDecoder);
    try {
      mRecord = mDatumReader.read(mRecord, mDecoder);
    } catch (ArrayIndexOutOfBoundsException oob) {
      // Could not read fields of this event.
      return;
    }

    // TODO: BAOS.toByteArray() creates a new byte array, as does the
    // creation of the event. That's at least one more array copy than
    // necessary.
    mOutputBytes.reset();
    mDatumWriter.write(mRecord, mEncoder);
    Event out = new EventImpl(mOutputBytes.toByteArray(),
        e.getTimestamp(), e.getPriority(), e.getNanos(), e.getHost()); 
    emit(out);
  }
}
