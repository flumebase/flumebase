// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.List;

import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * An expression which evaluates to a value inside a record.
 */
public abstract class Expr extends SQLStatement {


  /** @return a compact string representation of this expression without line breaks. */
  public abstract String toStringOneLine();

  /**
   * @return the type of this expression with a given set of symbols,
   * or null if no type can be reconciled.
   */
  public abstract Type getType(SymbolTable symTab);

  /**
   * @return the list of all TypedFields required to evaluate the expression.
   */
  public abstract List<TypedField> getRequiredFields(SymbolTable symTab);
}
