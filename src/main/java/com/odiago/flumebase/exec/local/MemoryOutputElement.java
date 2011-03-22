/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.FlowElementImpl;

import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.util.StringUtils;

import com.odiago.flumebase.util.concurrent.SelectableList;
import com.odiago.flumebase.util.concurrent.SyncSelectableList;

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
  private SelectableList<GenericData.Record> mOutputRecords;

  // Members used to decode Avro into fields.
  private Schema mOutputSchema;

  public MemoryOutputElement(FlowElementContext context, List<TypedField> fields) {
    super(context);

    mFields = fields;
    mOutputRecords = new SyncSelectableList<GenericData.Record>();
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
    mOutputRecords.add(outRecord);
  }

  /** @return the collected output records. */
  public SelectableList<GenericData.Record> getRecords() {
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
