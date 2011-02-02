// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.io;

import java.io.IOException;

import java.util.Map;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.lang.Type;

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

    if (mDecoderFactory == null) {
      // Avro is not yet initialized. Do so here.

      // TODO (aaron): This is technical debt. This is a weird place to do
      // this sort of initialization -- but that's because we don't want to
      // throw an exception in the c'tor. We should create a proper 'init(Map
      // params) throws IOE' method that we call in a place where we expect an
      // ioexception, and can immediately stop the stream's processing based
      // on this.
      String schemaStr = mParams.get(SCHEMA_PARAM);
      if (null == schemaStr) {
        throw new IOException("The EventParser for this stream requires the '"
            + SCHEMA_PARAM + "' property to be set. Try recreating the stream as: "
            + "CREATE STREAM .. EVENT FORMAT 'avro' PROPERTIES ('" + SCHEMA_PARAM
            + "' = ...)");
      }
      mSchema = Schema.parse(schemaStr);
      mDecoderFactory = new DecoderFactory();
      mRecord = new GenericData.Record(mSchema);
      mDatumReader = new GenericDatumReader<GenericData.Record>(mSchema);
    }

    if (!mIsDecoded) {
      // Now that we actually want a record value, decode the input bytes.
      mDecoder = mDecoderFactory.createBinaryDecoder(mEvent.getBody(), mDecoder);
      mRecord = mDatumReader.read(mRecord, mDecoder);
      mIsDecoded = true;
    }

    // TODO(aaron): This is more a user assert than a code assert. We need a way to
    // enable "strict mode" for input and "lazy mode."
    assert mSchema.getFields().get(colIdx).schema().equals(expectedType.getAvroSchema());
    return mRecord.get(colIdx);
  }


}
