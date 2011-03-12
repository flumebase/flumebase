// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.Bucket;

import com.odiago.flumebase.lang.AggregateFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return the min non-null value in a column.
 */
@SuppressWarnings("rawtypes")
public class min extends AggregateFunc<Comparable> {
  private UniversalType mArgType;

  public min() {
    // Argument may have any input type.
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE));
  }

  @Override
  public void addToBucket(Object arg, Bucket<Comparable> bucket, Type type) {
    assert type.isComparable();

    Comparable comparableArg = (Comparable) arg;
    if (null != comparableArg) {
      Object curMin = bucket.getState();
      if (curMin == null || comparableArg.compareTo(curMin) < 0) {
        // No currently defined min, or we just exceeded the observed min. Use this.
        bucket.setState(comparableArg);
      }
    }
  }

  @Override
  public Object finishWindow(Iterable<Bucket<Comparable>> buckets, Type type) {

    Comparable curMin = null;
    for (Bucket<Comparable> bucket : buckets) {
      Comparable state = bucket.getState();
      if (curMin == null || (null != state && state.compareTo(curMin) < 0)) {
        curMin = state;
      }
    }

    return curMin;
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
