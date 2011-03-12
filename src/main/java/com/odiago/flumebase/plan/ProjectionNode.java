// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.plan;

import java.util.ArrayList;
import java.util.List;

import com.odiago.flumebase.parser.TypedField;

import com.odiago.flumebase.util.StringUtils;

/**
 * Node that projects from an input schema onto an output set of fields.
 * The input schema is not explicitly specified; it is inferred from the
 * flow.
 */
public class ProjectionNode extends PlanNode {
  private List<TypedField> mInputFields;
  private List<TypedField> mOutputFields;

  public ProjectionNode(List<TypedField> inFields, List<TypedField> outFields) {
    mInputFields = new ArrayList<TypedField>(inFields);
    mOutputFields = new ArrayList<TypedField>(outFields);
  }

  public List<TypedField> getInputFields() {
    return mInputFields;
  }

  public List<TypedField> getOutputFields() {
    return mOutputFields;
  }

  @Override
  public void formatParams(StringBuilder sb) {
    sb.append("ProjectionNode inFields=(");
    StringUtils.formatList(sb, mInputFields);
    sb.append(") outFields=(");
    StringUtils.formatList(sb, mOutputFields);
    sb.append(")\n");

    formatAttributes(sb);
  }
}
