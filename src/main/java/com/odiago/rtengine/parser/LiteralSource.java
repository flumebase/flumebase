// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Specify a source for the FROM clause of a SELECT statement that
 * references the literal name of a stream.
 *
 * A LiteralSource is not an executable SQLStatement, but it shares
 * the common hierarchy.
 */
public class LiteralSource extends SQLStatement {
  private String mSourceName;

  public LiteralSource(String name) {
    mSourceName = name;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("Literal source: ");
    sb.append(mSourceName);
    sb.append("\n");
  }
}

