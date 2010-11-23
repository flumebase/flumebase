// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.util.List;

import org.apache.avro.Schema;

import com.odiago.rtengine.exec.FlowElementContext;

import com.odiago.rtengine.parser.TypedField;

/** Container for all the state an RtsqlSink needs to lazily initialize. */
public class SinkContext {
  private final FlowElementContext mFlowContext;
  private final Schema mOutputSchema;
  private final List<TypedField> mFieldTypes;

  public SinkContext(FlowElementContext flowContext, Schema outputSchema,
      List<TypedField> fieldTypes) {
    mFlowContext = flowContext;
    mOutputSchema = outputSchema;
    mFieldTypes = fieldTypes;
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
}
