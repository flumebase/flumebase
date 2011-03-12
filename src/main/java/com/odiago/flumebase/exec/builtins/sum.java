// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.Bucket;

import com.odiago.flumebase.lang.AggregateFunc;
import com.odiago.flumebase.lang.EvalException;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return the sum of values in a column. If all values are null, return null.
 */
public class sum extends AggregateFunc<Number> {
  private UniversalType mArgType;

  public sum() {
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC));
  }

  private Number getState(Bucket<Number> bucket) {
    Number state = bucket.getState();
    if (null == state) {
      state = Integer.valueOf(0);
    }

    return state;
  }
 
  @Override
  public void addToBucket(Object arg, Bucket<Number> bucket, Type type)
    throws EvalException {

    Number num = (Number) arg;
    Number state = getState(bucket);
    if (null == num) {
      // Don't add anything to the sum.
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      bucket.setState(state.intValue() + num.intValue());
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      bucket.setState(state.longValue() + num.longValue());
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      bucket.setState(state.floatValue() + num.floatValue());
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      bucket.setState(state.doubleValue() + num.doubleValue());
    } else {
      throw new EvalException("Cannot take sum over type: " + num.getClass().getName());
    }
  }

  public Object finishWindow(Iterable<Bucket<Number>> buckets, Type type)
    throws EvalException {

    boolean nonNull = false;
    if (type.getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      int total = 0;
      for (Bucket<Number> bucket : buckets) {
        Number state = bucket.getState();
        if (null != state) {
          total += state.intValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Integer.valueOf(total);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      long total = 0;
      for (Bucket<Number> bucket : buckets) {
        Number state = bucket.getState();
        if (null != state) {
          total += state.longValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Long.valueOf(total);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      float total = 0;
      for (Bucket<Number> bucket : buckets) {
        Number state = bucket.getState();
        if (null != state) {
          total += state.floatValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Float.valueOf(total);
      } else {
        return null; // Only null values in buckets.
      }
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      double total = 0;
      for (Bucket<Number> bucket : buckets) {
        Number state = bucket.getState();
        if (null != state) {
          total += state.doubleValue();
          nonNull = true;
        }
      }

      if (nonNull) {
        return Double.valueOf(total);
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
