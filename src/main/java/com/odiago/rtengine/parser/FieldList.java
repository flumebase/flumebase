// (c) Copyright 2010, Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

/**
 * A list of fields in a select statement, GROUP BY, etc.
 */
public class FieldList {
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

  public List<String> getFieldNames() {
    return mNames;
  }

  /**
   * Return a list of TypedField objects taken by resolving the literal names
   * in this field list against the specified symbol table.
   */
  public List<TypedField> getFields(SymbolTable symtab) {
    List<TypedField> out = new ArrayList<TypedField>();

    for (String name : mNames) {
      Symbol sym = symtab.resolve(name);
      out.add(new TypedField(name, sym.getType()));
    }

    return out;
  }

  public void addField(String fieldName) {
    mNames.add(fieldName);
  }
}
