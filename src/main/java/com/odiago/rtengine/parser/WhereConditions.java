// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import com.odiago.rtengine.exec.SymbolTable;

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

  /** @return a list of fields required by the WHERE clause.
   * The syntax for the clause knows the names of the required fields;
   * resolve them against the specified symbol table into TypedFields.
   */ 
  public List<TypedField> getRequiredFields(SymbolTable symtab) {
    return new ArrayList<TypedField>();
  }
}
