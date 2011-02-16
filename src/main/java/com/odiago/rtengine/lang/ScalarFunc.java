// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

/**
 * A scalar function that takes in a tuple of fixed arity and returns
 * a single value.
 *
 * <p>Instances of this class should be stateless; repeated calls to eval() to
 * apply the function to different sets of arguments should work without
 * regard to the order in which the calls are made.</p>
 */
public abstract class ScalarFunc extends Function {
  /**
   * Apply the function to its arguments and return its result.
   * @throws EvalException if the function cannot be evaluated (for example,
   * there are not enough arguments, etc.).
   */
  public abstract Object eval(Object... args) throws EvalException;
}
