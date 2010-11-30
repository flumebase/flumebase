// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.parser.TypedField;

import com.odiago.rtengine.util.StringUtils;

/**
 * Node that projects from an input schema onto an output set of fields.
 * The input schema is not explicitly specified; it is inferred from the
 * flow.
 */
public class ProjectionNode extends PlanNode {
  private List<TypedField> mFields;

  public ProjectionNode(List<TypedField> fields) {
    mFields = new ArrayList<TypedField>(fields);
  }

  public List<TypedField> getFields() {
    return mFields;
  }

  @Override
  public void formatParams(StringBuilder sb) {
    sb.append("ProjectionNode fields=(");
    StringUtils.formatList(sb, mFields);
    sb.append(")\n");
    formatAttributes(sb);
  }
}
