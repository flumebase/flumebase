// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.Bucket;

import com.odiago.rtengine.lang.AggregateFunc;
import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.UniversalType;

/**
 * Return the max non-null value in a column.
 */
@SuppressWarnings("rawtypes")
public class max extends AggregateFunc<Comparable> {
  private UniversalType mArgType;

  public max() {
    // Argument may have any input type.
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE));
  }

  @Override
  public void addToBucket(Object arg, Bucket<Comparable> bucket, Type type) {
    assert type.isComparable();

    Comparable comparableArg = (Comparable) arg;
    if (null != comparableArg) {
      Object curMax = bucket.getState();
      if (curMax == null || comparableArg.compareTo(curMax) > 0) {
        // No currently defined max, or we just exceeded the observed max. Use this.
        bucket.setState(comparableArg);
      }
    }
  }

  @Override
  public Object finishWindow(Iterable<Bucket<Comparable>> buckets, Type type) {
    Comparable curMax = null;
    for (Bucket<Comparable> bucket : buckets) {
      Comparable state = bucket.getState();
      if (curMax == null || (null != state && state.compareTo(curMax) > 0)) {
        curMax = state;
      }
    }

    return curMax;
  }

  @Override
  public Type getReturnType() {
    return mArgType; // Return type is the same as our argument.
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList((Type) mArgType);
  }
}
