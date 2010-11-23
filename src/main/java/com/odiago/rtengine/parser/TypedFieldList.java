// (c) Copyright 2010, Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A list of fields in a CREATE STREAM statement or other place where
 * fields are defined with types.
 */
public class TypedFieldList implements Iterable<TypedField> {
  private List<TypedField> mTypedFields;

  public TypedFieldList(TypedField firstField) {
    mTypedFields = new ArrayList<TypedField>();
    addField(firstField);
  }

  public Iterator<TypedField> iterator() {
    return mTypedFields.iterator();
  }

  public void addField(TypedField typedField) {
    mTypedFields.add(typedField);
  }
}
