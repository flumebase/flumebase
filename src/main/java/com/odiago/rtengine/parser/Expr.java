// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
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

  /**
   * Evaluate this expression, pulling identifiers from the input event wrapper.
   */
  public abstract Object eval(EventWrapper inWrapper) throws IOException;

  /**
   * @return the type of this node after type checking is complete.
   * The typechecker will set the type inside the node so it does not
   * need to rely on a symobl table at run time.
   */
  abstract Type getResolvedType();
}
