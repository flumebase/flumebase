// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.Bucket;

import com.odiago.rtengine.lang.AggregateFunc;
import com.odiago.rtengine.lang.EvalException;
import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.UniversalType;

/**
 * Return the arithmetic mean of values in a column. If all values are null, return null.
 *
 * Each bucket contains the sum and count of items in the bucket. The completion step
 * takes the weighted average of the buckets.
 */
public class avg extends AggregateFunc<AvgState> {
  private UniversalType mArgType;

  public avg() {
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC));
  }

  private AvgState getState(Bucket<AvgState> bucket) {
    AvgState state = bucket.getState();
    if (null == state) {
      state = new AvgState();
      bucket.setState(state);
    }

    return state;
  }
 
  @Override
  public void addToBucket(Object arg, Bucket<AvgState> bucket, Type type)
    throws EvalException {

    Number num = (Number) arg;
    AvgState state = getState(bucket);
    if (null == num) {
      // Don't add anything to the sum.
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      state.mCount++;
      state.mSum = state.mSum.intValue() + num.intValue();
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      state.mCount++;
      state.mSum = state.mSum.longValue() + num.longValue();
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      state.mCount++;
      state.mSum = state.mSum.floatValue() + num.floatValue();
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      state.mCount++;
      state.mSum = state.mSum.doubleValue() + num.doubleValue();
    } else {
      throw new EvalException("Cannot take sum over type: " + num.getClass().getName());
    }
  }

  public Object finishWindow(Iterable<Bucket<AvgState>> buckets, Type type)
    throws EvalException {

    int totalCount = 0;
    boolean nonNull = false;
    if (type.getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      int totalWeight = 0;
      for (Bucket<AvgState> bucket : buckets) {
        AvgState state = bucket.getState();
        if (null != state) {
          totalCount += state.mCount;
          totalWeight += state.mSum.intValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Integer.valueOf(totalWeight / totalCount);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      long totalWeight = 0;
      for (Bucket<AvgState> bucket : buckets) {
        AvgState state = bucket.getState();
        if (null != state) {
          totalCount += state.mCount;
          totalWeight += state.mSum.longValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Long.valueOf(totalWeight / (long) totalCount);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      float totalWeight = 0;
      for (Bucket<AvgState> bucket : buckets) {
        AvgState state = bucket.getState();
        if (null != state) {
          totalCount += state.mCount;
          totalWeight += state.mSum.floatValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Float.valueOf(totalWeight / (float) totalCount);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      double totalWeight = 0;
      for (Bucket<AvgState> bucket : buckets) {
        AvgState state = bucket.getState();
        if (null != state) {
          totalCount += state.mCount;
          totalWeight += state.mSum.doubleValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Double.valueOf(totalWeight / (double) totalCount);
      } else {
        return null; // Only null values in buckets.
      }
    } else {
      throw new EvalException("Don't know how to aggregate with type: " + type);
    }
  }

  @Override
  public Type getReturnType() {
    // Return type is same as the input argument.
    return mArgType;
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList((Type) mArgType);
  }

  @Override
  public boolean autoPromoteArguments() {
    return false;
  }
}
