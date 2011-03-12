// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.lang;

import java.util.List;

/**
 * Abstract base class that defines a callable function. Subclasses
 * of this exist for scalar, aggregate, and table functions.
 */
public abstract class Function {
  /**
   * @return the Type of the object returned by the function.
   */
  public abstract Type getReturnType();
  
  /**
   * @return an ordered list containing the types expected for all arguments.
   */
  public abstract List<Type> getArgumentTypes();

  /**
   * Determines whether arguments are promoted to their specified types by
   * the runtime. If this returns true, actual arguments are promoted to
   * new values that match the types specified in getArgumentTypes().
   * If false, the expressions are simply type-checked to ensure that there
   * is a valid promotion, but are passed in as-is. The default value of
   * this method is true.
   */
  public boolean autoPromoteArguments() {
    return true;
  }
}
