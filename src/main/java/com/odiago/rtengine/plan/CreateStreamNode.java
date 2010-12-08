// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.FormatSpec;
import com.odiago.rtengine.parser.StreamSourceType;
import com.odiago.rtengine.parser.TypedField;
import com.odiago.rtengine.parser.TypedFieldList;

import com.odiago.rtengine.util.StringUtils;

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
