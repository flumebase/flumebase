// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

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

import com.odiago.rtengine.io.ColumnParseException;
import com.odiago.rtengine.io.DelimitedEventParser;
import com.odiago.rtengine.io.EventParser;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.TypedField;

/** 
 * Class which extends FlowElementImpl with helper methods for
 * FlowElements that must parse their inputs (i.e., all "source" nodes).
 */
public class ParsingFlowElementImpl extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      ParsingFlowElementImpl.class.getName());

  // TODO(aaron): This class should be used in a way that allows for lazier
  // parsing. Right now, we parse every field and stuff it into a complete
  // record. We should push projection and some initial filtering up into the
  // source's FlowElement.

  /** The fields we need to parse out of each record, with their types. */
  private List<TypedField> mFieldTypes;

  /** The EventParser that returns our parsed fields from the input. */
  private EventParser mEventParser;

  // Avro components we reuse with each call.
  private ByteArrayOutputStream mOutputBytes;
  private BinaryEncoder mEncoder;
  private GenericDatumWriter<GenericRecord> mGenericWriter;
  private GenericData.Record mRecord;

  /**
   * Constructor that configures the EventParser
   * @param ctxt the FlowElementContext to pass to FlowElementImpl's
   * constructor.
   * @param outputSchema the Avro output schema to emit.
   * @param outputFields the names/types of fields to emit.
   */
  public ParsingFlowElementImpl(FlowElementContext ctxt, Schema outputSchema,
      List<TypedField> outputFields) {
    super(ctxt);
    mOutputBytes = new ByteArrayOutputStream();
    mGenericWriter = new GenericDatumWriter<GenericRecord>(outputSchema);
    mEncoder = new BinaryEncoder(mOutputBytes);
    mRecord = new GenericData.Record(outputSchema);

    // TODO(aaron): This class should allow parameterization of the type of
    // parser instantiated.
    mEventParser = new DelimitedEventParser();
    mFieldTypes = outputFields;
  }

  protected Event parseEvent(Event in) throws IOException {
    mEventParser.reset(in);
    for (int i = 0; i < mFieldTypes.size(); i++) {
      TypedField field = mFieldTypes.get(i);
      Type fieldType = field.getType();
      try {
        Object fieldData = mEventParser.getColumn(i, fieldType);
        mRecord.put(field.getName(), fieldData);
      } catch (ColumnParseException cpe) {
        // We couldn't parse the field - it may not exist in the input,
        // or it may not be parsible according to the field's type.
        if (fieldType.isNullable()) {
          // Store a null in this field. This is ok.
          mRecord.put(field.getName(), null);
        } else {
          // Discard this input record; it cannot be parsed.
          if (LOG.isDebugEnabled()) {
            LOG.debug("Could not parse field " + field.getName() + ": " + cpe);
          }
          return null;
        }
      }
    }

    // We've filled the record with fields; write the Avro data to the output sink.
    mOutputBytes.reset();
    mGenericWriter.write(mRecord, mEncoder);
    Event out = new EventImpl(mOutputBytes.toByteArray(),
        in.getTimestamp(), in.getPriority(), in.getNanos(), in.getHost());
    return out;
  }

  @Override
  public void takeEvent(Event e) throws IOException, InterruptedException {
    Event parsed = parseEvent(e);
    if (null != parsed) {
      emit(e);
    }
  }
}
