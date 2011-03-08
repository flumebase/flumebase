// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Represents an expression with a human-readable label, as well as
 * a unique identifier to pack into Avro records for transmitting
 * the result of the expression.
 */
public class AliasedExpr extends SQLStatement {
  /** The expression we're wrapping. */
  private Expr mExpr;

  /** The user-displayed label. */
  private String mDisplayLabel;

  /** The label to apply to the field of a serialized record. */
  private String mAvroLabel;

  /** The user-selected internal label. */
  private String mUserAlias;

  public AliasedExpr(Expr e) {
    mExpr = e;
  }

  public Expr getExpr() {
    return mExpr;
  }

  public void setExpr(Expr expr) {
    mExpr = expr;
  }

  /**
   * @return the label to present to the user for this column.
   */
  public String getDisplayLabel() {
    return mDisplayLabel;
  }

  /**
   * @return the label to use for this field in a serialized record.
   */
  public String getAvroLabel() {
    return mAvroLabel;
  }

  /**
   * @return the identifier to use when using this field in other expressions.
   */
  public String getUserAlias() {
    return mUserAlias;
  }

  public void setDisplayLabel(String displayLabel) {
    mDisplayLabel = displayLabel;
  }

  public void setAvroLabel(String avroLabel) {
    mAvroLabel = avroLabel;
  }

  public void setUserAlias(String userAlias) {
    mUserAlias = userAlias;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("AliasedExpr\n");
    pad(sb, depth + 1);
    sb.append("mDisplayLabel=");
    sb.append(mDisplayLabel);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("mAvroLabel=");
    sb.append(mAvroLabel);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("mUserAlias=");
    sb.append(mUserAlias);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("wrapped expr:\n");
    mExpr.format(sb, depth + 2);
  }

}
