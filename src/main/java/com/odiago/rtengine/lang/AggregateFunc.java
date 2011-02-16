// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.exec.Bucket;

/**
 * An aggregate function that receives multiple values for a single field
 * and returns a single aggregate value computed over that field for a
 * window of time.
 *
 * <p>Instances of this class should be stateless; repeated calls to eval() to
 * apply the function to different sets of arguments should work without
 * regard to the order in which the calls are made.</p>
 *
 * <p>AggregateFunc instances operate over time- or rowid-based buckets.
 * Each Bucket contains information regarding the bucket's time range,
 * etc. as well as a function-specified BUCKETSTATE object where the function
 * stores its data.</p>
 */
public abstract class AggregateFunc<BUCKETSTATE> extends Function {

  /**
   * Add 'arg' to the state for the time bucket which holds the argument.
   * @param arg a value for the column under aggregation
   * @param bucket the bucket into which the partial aggregate is stored
   * @param type the expected output type for this aggregate function. May be
   * different than the expected input type of 'arg'.
   * @throws EvalException if the function cannot be evaluated (for example,
   * the arg is of the wrong type, etc.).
   */
  public abstract void addToBucket(Object arg, Bucket<BUCKETSTATE> bucket, Type type)
      throws EvalException;

  /**
   * A time window spanning one or more buckets is ending; iterate over the buckets
   * and compute the function's final value for the time window.
   * @param buckets the set of buckets constituting the window.
   * @param type the expected output type for this function.
   * @return the final value for this function over a given time window.
   * @throws EvalException if the function cannot be evaluated.
   */
  public abstract Object finishWindow(Iterable<Bucket<BUCKETSTATE>> buckets, Type type)
      throws EvalException;
}
