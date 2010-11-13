// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * CREATE STREAM statement.
 */
public class CreateStreamStmt extends SQLStatement {
  private String mName;

  public CreateStreamStmt(String streamName) {
    mName = streamName;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("CREATE STREAM mName=");
    sb.append(mName);
    sb.append("\n");
  }
}

