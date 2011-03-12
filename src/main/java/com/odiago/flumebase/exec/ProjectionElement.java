// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import com.odiago.flumebase.parser.TypedField;

/**
 * Transform each binary-encoded Avro event we receive, projecting from its
 * input schema to our (narrower) output schema.
 */
public class ProjectionElement extends AvroOutputElementImpl {
  private List<TypedField> mInputFields;
  private List<TypedField> mOutputFields;

  public ProjectionElement(FlowElementContext ctxt, Schema outputSchema,
      List<TypedField> inputFields, List<TypedField> outputFields) {
    super(ctxt, outputSchema);
    mInputFields = new ArrayList<TypedField>(inputFields);
    mOutputFields = new ArrayList<TypedField>(outputFields);

    assert(mInputFields.size() == mOutputFields.size());
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    GenericData.Record record = new GenericData.Record(getOutputSchema());

    for (int i = 0; i < mInputFields.size(); i++) {
      TypedField inField = mInputFields.get(i);
      TypedField outField = mOutputFields.get(i);

      String outFieldName = outField.getAvroName();
      record.put(outFieldName, e.getField(inField));
    }

    emitAvroRecord(record, e.getEvent());
  }
}
