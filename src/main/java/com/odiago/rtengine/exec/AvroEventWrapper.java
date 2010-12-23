// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.parser.TypedField;

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
