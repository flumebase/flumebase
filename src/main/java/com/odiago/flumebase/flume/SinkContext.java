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

package com.odiago.flumebase.flume;

import java.util.List;

import org.apache.avro.Schema;

import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.parser.TypedField;

/** Container for all the state an RtsqlSink needs to lazily initialize. */
public class SinkContext {
  private final FlowElementContext mFlowContext;
  private final Schema mOutputSchema;
  private final List<TypedField> mFieldTypes;
  private final StreamSymbol mStreamSymbol;

  public SinkContext(FlowElementContext flowContext, Schema outputSchema,
      List<TypedField> fieldTypes, StreamSymbol streamSymbol) {
    mFlowContext = flowContext;
    mOutputSchema = outputSchema;
    mFieldTypes = fieldTypes;
    mStreamSymbol = streamSymbol;
  }

  public FlowElementContext getFlowElementContext() {
    return mFlowContext;
  }

  public Schema getOutputSchema() {
    return mOutputSchema;
  }

  public List<TypedField> getFieldTypes() {
    return mFieldTypes;
  }

  public StreamSymbol getStreamSymbol() {
    return mStreamSymbol;
  }
}
