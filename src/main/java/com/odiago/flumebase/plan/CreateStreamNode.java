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

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.FormatSpec;
import com.odiago.flumebase.parser.StreamSourceType;
import com.odiago.flumebase.parser.TypedField;
import com.odiago.flumebase.parser.TypedFieldList;

import com.odiago.flumebase.util.StringUtils;

/**
 * DDL operation that creates a stream.
 * Parameters here have the same types and definitions as in CreateStreamStmt,
 * although strings are already unquoted.
 */
public class CreateStreamNode extends PlanNode {
  private String mStreamName;
  private StreamSourceType mType;
  private String mSrcLocation;
  private boolean mIsLocal;
  private List<TypedField> mFieldTypes;
  private FormatSpec mFormatSpec;

  public CreateStreamNode(String streamName, StreamSourceType srcType,
       String sourceLocation, boolean isLocal, TypedFieldList fieldTypes,
       FormatSpec formatSpec) {
    mStreamName = streamName;
    mType = srcType;
    mSrcLocation = sourceLocation;
    mIsLocal = isLocal;
    mFieldTypes = new ArrayList<TypedField>();
    for (TypedField field : fieldTypes) {
      mFieldTypes.add(field);
    }
    mFormatSpec = formatSpec;
  }

  public FormatSpec getFormatSpec() {
    return mFormatSpec;
  }

  public String getName() {
    return mStreamName;
  }

  public StreamSourceType getType() {
    return mType;
  }

  public String getSource() {
    return mSrcLocation;
  }

  public boolean isLocal() {
    return mIsLocal;
  }

  /**
   * @return a list of TypedField instances declaring the types of all the fields
   * in the stream. Neither the list nor its constituent objects should be modified.
   */
  public List<TypedField> getFields() {
    return mFieldTypes;
  }

  /**
   * @return a new list of Type objects representing the unnamed fields; this is
   * computed based on the TypedField list stored internally.
   */
  public List<Type> getFieldsAsTypes() {
    List<Type> types = new ArrayList<Type>();
    for (TypedField field : mFieldTypes) {
      types.add(field.getType());
    }
    return types;
  }

  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("CreateStream name=");
    sb.append(mStreamName);
    sb.append(", mType=");
    sb.append(mType);
    sb.append(", mSrcLocation=\"");
    sb.append(mSrcLocation);
    sb.append("\", mIsLocal=");
    sb.append(mIsLocal);
    sb.append(" fields=(");
    StringUtils.formatList(sb, mFieldTypes);
    sb.append("), format=");
    sb.append(mFormatSpec.getFormat());
    sb.append("\n");
    formatAttributes(sb);
  }
}
