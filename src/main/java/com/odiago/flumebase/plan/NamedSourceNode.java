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

import java.util.List;

import com.odiago.flumebase.parser.TypedField;

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
