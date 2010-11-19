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

  public CreateStreamStmt(String streamName, StreamSourceType srcType,
      String sourceLocation, boolean isLocal) {
    mName = streamName;
    mType = srcType;
    mSrcLocation = sourceLocation;
    mIsLocal = isLocal;
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
  }

  @Override
  public void createExecPlan(PlanContext planContext) {
    // The execution plan for a CREATE STREAM statement is to
    // perform the DDL operation by itself and quit.

    planContext.getFlowSpec().addRoot(new CreateStreamNode(mName,
        mType, mSrcLocation, mIsLocal));
  }
}

