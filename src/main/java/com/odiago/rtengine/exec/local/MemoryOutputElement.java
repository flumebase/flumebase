// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that stores input events in a list that can be retrieved later.
 */
public class MemoryOutputElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      MemoryOutputElement.class.getName());
  private List<String> mFieldNames;
  private List<GenericData.Record> mOutputRecords;

  // Members used to decode Avro into fields.
  private DecoderFactory mDecoderFactory;
  private BinaryDecoder mDecoder;
  private GenericDatumReader<GenericData.Record> mGenericReader;

  public MemoryOutputElement(FlowElementContext context, Schema inputSchema,
      List<String> fieldNames) {
    super(context);

    mDecoderFactory = new DecoderFactory();
    mGenericReader = new GenericDatumReader<GenericData.Record>(inputSchema);

    mFieldNames = fieldNames;
    mOutputRecords = new ArrayList<GenericData.Record>();
  }

  @Override
  public void takeEvent(Event e) throws IOException {
    StringBuilder sb = new StringBuilder();
    long ts = e.getTimestamp();
    sb.append(ts);

    // Extract the Avro record from the event.
    mDecoder = mDecoderFactory.createBinaryDecoder(e.getBody(), mDecoder);
    GenericData.Record record = mGenericReader.read(null, mDecoder);
    mOutputRecords.add(record);
  }

  /** @return the collected output records. */
  public List<GenericData.Record> getRecords() {
    return mOutputRecords;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MemoryOutput(");
    StringUtils.formatList(sb, mFieldNames);
    sb.append(")");
    return sb.toString();
  }
}
