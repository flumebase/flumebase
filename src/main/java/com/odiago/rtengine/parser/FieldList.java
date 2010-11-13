// (c) Copyright 2010, Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A list of fields in a select statement, GROUP BY, etc.
 */
public class FieldList implements Iterable<String> {
  private List<String> mNames;

  protected FieldList() {
    // Intentionally leave mNames null if this is called
    // from a subclass; that means it's an All-Fields list.
  }

  public FieldList(String firstFieldName) {
    mNames = new ArrayList<String>();
    addField(firstFieldName);
  }

  /**
   * @return true if this is a "*"
   */
  public boolean isAllFields() {
    return false;
  }

  @Override
  public Iterator<String> iterator() {
    return mNames.iterator();
  }

  public void addField(String fieldName) {
    mNames.add(fieldName);
  }
}
