// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * FlowElement that stores input events in a list that can be retrieved later.
 * Returns a generic record based on the <i>display name</i> of each output
 * typed field, rather than the assigned avro name. This allows user-friendly
 * extraction of output fields, rather than having to infer the correct
 * avro label for each field.
 */
public class MemoryOutputElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      MemoryOutputElement.class.getName());
  private List<TypedField> mFields;
  private List<GenericData.Record> mOutputRecords;

  // Members used to decode Avro into fields.
  private Schema mOutputSchema;

  public MemoryOutputElement(FlowElementContext context, List<TypedField> fields) {
    super(context);

    mFields = fields;
    mOutputRecords = new ArrayList<GenericData.Record>();
    mOutputSchema = getOutputSchema(fields);
  }

  private Schema getOutputSchema(List<TypedField> fields) {
    List<Schema.Field> avroFields = new ArrayList<Schema.Field>();
    for (TypedField typedField : fields) {
      avroFields.add(new Schema.Field(typedField.getDisplayName(),
          typedField.getType().getAvroSchema(), null, null));
    }

    return Schema.createRecord(avroFields);
  }

  @Override
  public void takeEvent(EventWrapper wrapper) throws IOException {
    // Parse out all the fields into the new Avro record.
    GenericData.Record outRecord = new GenericData.Record(mOutputSchema);
    for (TypedField field : mFields) {
      outRecord.put(field.getDisplayName(), wrapper.getField(field));
    }
    LOG.info("Spun up: " + outRecord);
    mOutputRecords.add(outRecord);
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
