// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import com.odiago.rtengine.lang.TimeSpan;

import com.odiago.rtengine.parser.Expr;

/**
 * Join two input streams into an output stream based on an
 * expression criterion within a range interval.
 *
 * Requires that the test expression relies on a single "key" column
 * from each of the two input streams.
 */
public class HashJoinNode extends PlanNode {
  private String mLeftName; // name of the left stream.
  private String mRightName; // name of the right stream.
  private String mLeftKey; // name of the key field from the left stream.
  private String mRightKey; // name of the key field from the right stream.
  private Expr mJoinExpr; // expression to evaluate for each cross-pair of the input.
  private TimeSpan mWindowWidth; // timespan over which the join is valid.


  public HashJoinNode(String leftName, String rightName, String leftKey, String rightKey,
      Expr joinExpr, TimeSpan windowWidth) {
    mLeftName = leftName;
    mRightName = rightName;
    mLeftKey = leftKey;
    mRightKey = rightKey;
    mJoinExpr = joinExpr;
    mWindowWidth = windowWidth;
  }

  protected void formatParams(StringBuilder sb) {
    sb.append("Join mLeftName=");
    sb.append(mLeftName);
    sb.append(", mRightName=");
    sb.append(mRightName);
    sb.append(", mLeftKey=");
    sb.append(mLeftKey);
    sb.append(", mRightKey=");
    sb.append(mRightKey);
    sb.append(", width=");
    sb.append(mWindowWidth);
    sb.append(", expr=");
    sb.append(mJoinExpr.toStringOneLine());
    formatAttributes(sb);
  }
}
