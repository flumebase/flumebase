// (c) Copyright 2010 Odiago, Inc.

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
