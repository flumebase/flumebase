// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.AvroEventWrapper;
import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that stores input events in a list that can be retrieved later.
 */
public class MemoryOutputElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      MemoryOutputElement.class.getName());
  private List<TypedField> mFields;
  private List<GenericData.Record> mOutputRecords;

  // Members used to decode Avro into fields.
  private Schema mInputSchema;

  public MemoryOutputElement(FlowElementContext context, Schema inputSchema,
      List<TypedField> fields) {
    super(context);

    mInputSchema = inputSchema;
    mFields = fields;
    mOutputRecords = new ArrayList<GenericData.Record>();
  }

  @Override
  public void takeEvent(EventWrapper wrapper) throws IOException {
    if (wrapper instanceof AvroEventWrapper) {
      // Pull the record directly from here.
      GenericData.Record record = ((AvroEventWrapper) wrapper).getRecord();
      LOG.info("Take Avro event direct: " + record);
      mOutputRecords.add(record);
    } else {
      // Parse out all the fields into Avro ourselves.
      GenericData.Record outRecord = new GenericData.Record(mInputSchema);
      for (TypedField field : mFields) {
        outRecord.put(field.getAvroName(), wrapper.getField(field));
      }
      LOG.info("Spun up: " + outRecord);
      mOutputRecords.add(outRecord);
    }
  }

  /** @return the collected output records. */
  public List<GenericData.Record> getRecords() {
    return mOutputRecords;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MemoryOutput(");
    StringUtils.formatList(sb, mFields);
    sb.append(")");
    return sb.toString();
  }
}
