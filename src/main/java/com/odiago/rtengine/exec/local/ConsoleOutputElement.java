// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that prints events to the console.
 */
public class ConsoleOutputElement extends FlowElementImpl {
  private List<String> mFieldNames;

  // Members used to decode Avro into fields.
  private DecoderFactory mDecoderFactory;
  private BinaryDecoder mDecoder;
  private GenericData.Record mRecord;
  private GenericDatumReader<GenericData.Record> mGenericReader;

  public ConsoleOutputElement(FlowElementContext context, Schema inputSchema,
      List<String> fieldNames) {
    super(context);

    mDecoderFactory = new DecoderFactory();
    mRecord = new GenericData.Record(inputSchema);
    mGenericReader = new GenericDatumReader<GenericData.Record>(inputSchema);

    mFieldNames = fieldNames;

    printHeader();
  }

  private void printHeader() {
    StringBuilder sb = new StringBuilder();
    sb.append("timestamp");
    for (String field : mFieldNames) {
      sb.append("\t");
      sb.append(field);
    }
    System.out.println(sb);
  }

  @Override
  public void takeEvent(Event e) throws IOException {
    StringBuilder sb = new StringBuilder();
    long ts = e.getTimestamp();
    sb.append(ts);

    // Extract the Avro record from the event.
    mDecoder = mDecoderFactory.createBinaryDecoder(e.getBody(), mDecoder);
    mRecord = mGenericReader.read(mRecord, mDecoder);
    for (String field : mFieldNames) {
      sb.append('\t');
      Object fieldVal = mRecord.get(field);
      if (null == fieldVal) {
        sb.append("null");
      } else {
        sb.append(fieldVal);
      }
    }
    System.out.println(sb.toString());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ConsoleOutput(");
    StringUtils.formatList(sb, mFieldNames);
    sb.append(")");
    return sb.toString();
  }
}
