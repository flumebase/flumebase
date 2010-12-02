// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Expression returning the value of a named field or alias.
 */
public class IdentifierExpr extends Expr {

  private String mIdentifier;

  public IdentifierExpr(String identifier) {
    mIdentifier = identifier;
  }

  public String getIdentifier() {
    return mIdentifier;
  }

  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("IdentifierExpr mIdentifier=");
    sb.append(mIdentifier);
    sb.append("\n");
  }
}
