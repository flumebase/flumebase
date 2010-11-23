// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import com.odiago.rtengine.parser.TypedField;

/**
 * Input source that reads from a source with a name already defined
 * in the symbol table.
 */
public class NamedSourceNode extends PlanNode {
  private String mStreamName;
  private List<TypedField> mFields;

  public NamedSourceNode(String streamName, List<TypedField> fields) {
    mStreamName = streamName;
    mFields = fields;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("NamedSource streamName=");
    sb.append(mStreamName);
    sb.append("\n");
    for (TypedField field : mFields) {
      sb.append("  ");
      sb.append(field.toString());
      sb.append("\n");
    }
    formatAttributes(sb);
  }

  public String getStreamName() {
    return mStreamName;
  }

  public List<TypedField> getFields() {
    return mFields;
  }
}
