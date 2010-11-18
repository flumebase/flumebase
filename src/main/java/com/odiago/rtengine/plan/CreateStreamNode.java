// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import com.odiago.rtengine.parser.StreamSourceType;

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

  public CreateStreamNode(String streamName, StreamSourceType srcType,
       String sourceLocation, boolean isLocal) {
    mStreamName = streamName;
    mType = srcType;
    mSrcLocation = sourceLocation;
    mIsLocal = isLocal;
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
  }
}
