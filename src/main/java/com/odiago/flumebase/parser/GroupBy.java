// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.parser;

import java.util.List;

/**
 * GROUP BY clause attached to a SELECT statement.
 */
public class GroupBy extends SQLStatement {

  // Field names to use for grouping.
  private List<String> mFieldNames;

  // TypedField list of query-assigned names and types for all fields
  // in mFieldNames; assigned by typechecker.
  private List<TypedField> mFieldTypes;

  public GroupBy(List<String> fieldNames) {
    mFieldNames = fieldNames;
  }

  public List<String> getFieldNames() {
    return mFieldNames;
  }

  public List<TypedField> getFieldTypes() {
    return mFieldTypes;
  }

  public void setFieldTypes(List<TypedField> fieldTypes) {
    mFieldTypes = fieldTypes;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("GROUP BY\n");
    pad(sb, depth + 1);
    sb.append("fields: ");
    boolean first = true;
    for (String fieldName : mFieldNames) {
      if (!first) {
        sb.append(", ");
      }

      sb.append(fieldName);
      first = false;
    }
    sb.append("\n");
  }
}
