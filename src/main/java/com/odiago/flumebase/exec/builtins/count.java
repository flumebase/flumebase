// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.Bucket;

import com.odiago.flumebase.lang.AggregateFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return the count of non-null values in a column.
 */
public class count extends AggregateFunc<Integer> {
  private UniversalType mArgType;

  public count() {
    // Argument may have any input type.
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_ANY));
  }

  private Integer getState(Bucket<Integer> bucket) {
    Integer state = bucket.getState();
    if (null == state) {
      state = Integer.valueOf(0);
    }

    return state;
  }
 
  @Override
  public void addToBucket(Object arg, Bucket<Integer> bucket, Type type) {
    assert type.equals(Type.getPrimitive(Type.TypeName.INT));

    if (null != arg) {
      Integer state = getState(bucket);
      int val = 0;
      if (null != state) {
        val = state.intValue();
      }

      val++; // Non-null item in bucket increments state.
      bucket.setState(Integer.valueOf(val));
    }
  }

  @Override
  public Object finishWindow(Iterable<Bucket<Integer>> buckets, Type type) {
    int total = 0;
    for (Bucket<Integer> bucket : buckets) {
      Integer state = bucket.getState();
      if (null != state) {
        total += state.intValue();
      }
    }

    return Integer.valueOf(total);
  }

  @Override
  public Type getReturnType() {
    return Type.getPrimitive(Type.TypeName.INT);
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList((Type) mArgType);
  }
}
