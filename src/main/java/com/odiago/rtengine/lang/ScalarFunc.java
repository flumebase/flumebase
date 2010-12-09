// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.List;

/**
 * A scalar function that takes in a tuple of fixed arity and returns
 * a single value.
 *
 * <p>Instances of this class should be stateless; repeated calls to eval() to
 * apply the function to different sets of arguments should work without
 * regard to the order in which the calls are made.</p>
 */
public abstract class ScalarFunc {
  /**
   * @return the Type of the object returned by the function.
   */
  public abstract Type getReturnType();

  /**
   * Apply the function to its arguments and return its result.
   * @throws EvalException if the function cannot be evaluated (for example,
   * there are not enough arguments, etc.).
   */
  public abstract Object eval(Object... args) throws EvalException;
  
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
