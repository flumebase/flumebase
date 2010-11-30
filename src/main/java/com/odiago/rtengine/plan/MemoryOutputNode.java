// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * Node that emits specific fields from all input records to a memory buffer.
 */
public class MemoryOutputNode extends PlanNode {
  
  /** The set of field names and types to emit to the console. */
  private List<TypedField> mOutputFields;

  /**
   * The name to bind the MemoryOutputElement to, for later retrieval
   * by the client.
   */
  private String mBufferName;

  public MemoryOutputNode(String memoryBufferName, List<TypedField> fields) {
    mBufferName = memoryBufferName;
    mOutputFields = fields;
  }

  public List<TypedField> getFields() {
    return mOutputFields;
  }

  public String getName() {
    return mBufferName;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("MemoryOutput(mBufferName=");
    sb.append(mBufferName);
    sb.append(", mOutputFields=(");
    StringUtils.formatList(sb, mOutputFields);
    sb.append("))\n");
    formatAttributes(sb);
  }
}
