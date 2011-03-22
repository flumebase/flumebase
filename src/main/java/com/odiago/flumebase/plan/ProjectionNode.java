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
