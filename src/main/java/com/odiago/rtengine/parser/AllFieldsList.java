// (c) Copyright 2010, Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * A list of fields in a select statement, GROUP BY, etc
 * that represents the "all fields" token '*'.
 */
public class AllFieldsList extends FieldList {
  public AllFieldsList() {
  }

  /**
   * @return true if this is a "*", which this is.
   */
  public boolean isAllFields() {
    return true;
  }
}
