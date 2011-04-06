/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec.builtins;

import java.math.BigDecimal;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.Bucket;

import com.odiago.flumebase.lang.AggregateFunc;
import com.odiago.flumebase.lang.EvalException;
import com.odiago.flumebase.lang.PreciseType;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

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
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.PRECISE)) {
      state.mCount++;
      if (state.mSum instanceof Integer) {
        state.mSum = PreciseType.toPreciseType(type).parseStringInput("0");
      }
      state.mSum = ((BigDecimal) state.mSum).add((BigDecimal) num);
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
    } else if (type.getPrimitiveTypeName().equals(Type.TypeName.PRECISE)) {
      BigDecimal totalWeight = PreciseType.toPreciseType(type).parseStringInput("0");
      for (Bucket<AvgState> bucket : buckets) {
        AvgState state = bucket.getState();
        if (null != state) {
          totalCount += state.mCount;
          if (state.mSum instanceof Integer) {
            totalWeight = totalWeight.add(BigDecimal.valueOf(((Integer) state.mSum).intValue()));
          } else {
            totalWeight = totalWeight.add((BigDecimal) state.mSum);
          }
          nonNull = true;
        }
      }

      if (nonNull) {
        return totalWeight.divide(BigDecimal.valueOf(totalCount));
      } else {
        return null;
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
