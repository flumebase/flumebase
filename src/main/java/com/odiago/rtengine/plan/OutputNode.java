// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * Node that emits specific fields from all input records to consoles, Flume, etc.
 */
public class OutputNode extends PlanNode {
  
  /** The set of field names and types to emit to the console. */
  private List<TypedField> mInputFields;

  /** The set of field names to inject into Avro records for Flume. */
  private List<TypedField> mOutputFields;

  /** Name of the Flume node to broadcast results through. */
  private String mFlumeNodeName;

  public OutputNode(List<TypedField> inputFields, List<TypedField> outputFields,
      String flumeNodeName) {
    mInputFields = inputFields;
    mOutputFields = outputFields;
    mFlumeNodeName = flumeNodeName;
  }

  public String getFlumeNodeName() {
    return mFlumeNodeName;
  }

  public List<TypedField> getInputFields() {
    return mInputFields;
  }

  public List<TypedField> getOutputFields() {
    return mOutputFields;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("Output(");
    StringUtils.formatList(sb, mInputFields);
    if (mFlumeNodeName != null) {
      sb.append(" -> ");
      sb.append(mFlumeNodeName);
    }
    sb.append(")\n");
    formatAttributes(sb);
  }
}
