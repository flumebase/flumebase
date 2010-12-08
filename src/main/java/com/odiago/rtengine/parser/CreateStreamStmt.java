// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.plan.CreateStreamNode;
import com.odiago.rtengine.plan.PlanContext;

/**
 * CREATE STREAM statement.
 */
public class CreateStreamStmt extends SQLStatement {

  /** The name of the STREAM object in RTSQL. */
  private String mName;
  /** The type of the stream's source (file, flume sink, etc.) */
  private StreamSourceType mType;
  /** The location of the source (path to file, flume eventsource description, etc.) */
  private String mSrcLocation;
  /** True if this is a local fs file, or an embedded flume node. */
  private boolean mIsLocal;

  /** Set of fields and types within each event in the stream. */
  private TypedFieldList mFields;

  /** Holds the event parsing format and properties. */
  private FormatSpec mFormatSpec;

  public CreateStreamStmt(String streamName, StreamSourceType srcType,
      String sourceLocation, boolean isLocal, TypedFieldList fields) {
    mName = streamName;
    mType = srcType;
    mSrcLocation = sourceLocation;
    mIsLocal = isLocal;
    mFields = fields;
    mFormatSpec = new FormatSpec(FormatSpec.DEFAULT_FORMAT_NAME);
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("CREATE STREAM mName=");
    sb.append(mName);
    sb.append(", mType=");
    sb.append(mType);
    sb.append(", mSrcLocation=\"");
    sb.append(mSrcLocation);
    sb.append("\", mIsLocal=");
    sb.append(mIsLocal);
    sb.append("\n");
    for (TypedField field : mFields) {
      pad(sb, depth + 2);
      sb.append(field.toString());
      sb.append("\n");
    }
    mFormatSpec.format(sb, depth + 1);
  }

  /**
   * If the user provides a more specific formatspec, set it here.
   */
  public void setFormatSpec(FormatSpec spec) {
    mFormatSpec = spec;
  }

  public FormatSpec getFormatSpec() {
    return mFormatSpec;
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // The execution plan for a CREATE STREAM statement is to
    // perform the DDL operation by itself and quit.

    planContext.getFlowSpec().addRoot(new CreateStreamNode(mName,
        mType, mSrcLocation, mIsLocal, mFields, mFormatSpec));
    return planContext;
  }
}

