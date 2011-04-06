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
      record.put(outFieldName, nativeToAvro(e.getField(inField)));
    }

    emitAvroRecord(record, e.getEvent());
  }
}
