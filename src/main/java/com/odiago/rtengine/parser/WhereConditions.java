// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

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

  /** @return a list of fields required by the WHERE clause. */
  public List<String> getRequiredFields() {
    return new ArrayList<String>();
  }
}
