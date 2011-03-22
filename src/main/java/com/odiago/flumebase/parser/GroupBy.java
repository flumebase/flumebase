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
