// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * WHERE conditions for a SELECT (or other) statement.
 * Current implementation is just a string, which is a regex that things 
 * have to match. TODO(aaron): Turn this into a proper list of boolean exps. 
 */
public class WhereConditions {
  private String mText;

  public WhereConditions(String text) {
    mText = text;
  }

  public String getText() {
    return mText;
  }
}
