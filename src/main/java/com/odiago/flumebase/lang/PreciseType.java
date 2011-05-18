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

package com.odiago.flumebase.lang;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.avro.Schema;

/**
 * Represents a PRECISE(n) type.
 *
 * <p>This type, while scalar, is parameterized by a value 'n' which
 * specifies the degree of precision associated with the value. Internally
 * this type is implemented as BigDecimal. The parameter 'n' corresponds to
 * the 'scale' associated with the BigDecimal.
 * </p>
 */
public class PreciseType extends Type implements Comparable<PreciseType> {

  /** The BigDecimal scale associated with this type. */
  private int mScale;

  public PreciseType(int scale) {
    super(TypeName.PRECISE);
    this.mScale = scale;
  }

  public int getScale() {
    return mScale;
  }

  @Override
  public String toString(boolean isNullable) {
    if (isNullable) {
      return "PRECISE(" + mScale + ")";
    } else {
      return "PRECISE(" + mScale + ") NOT NULL";
    }
  }

  @Override
  public Schema getAvroSchema() {
    // TODO: Is there a more efficient representation here?
    return Schema.create(Schema.Type.STRING);
  }

  @Override
  public int hashCode() {
    return Type.TypeName.PRECISE.hashCode() * 5 + 11 * mScale;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other == null) {
      return false;
    } else if (!other.getClass().equals(getClass())) {
      return false;
    }

    PreciseType otherType = (PreciseType) other;
    return mScale == otherType.mScale;
  }

  /**
   * Given a string representation of a precise type value, return the BigDecimal
   * that actually holds the precise value we will work with.
   */
  public BigDecimal parseStringInput(String in) {
    BigDecimal bigD = new BigDecimal(in);
    return bigD.setScale(mScale, RoundingMode.HALF_EVEN);
  }

  /**
   * Given a Type instance that may be a Nullable(Precise) type, or a
   * PreciseType, extract the Precise component and return it.
   *
   * @throws RuntimeException if the Type object argument cannot be converted.
   */
  public static PreciseType toPreciseType(Type t) {
    if (t.isNullable()) {
      Type inner = ((NullableType) t).getInnerType();
      if (inner instanceof PreciseType) {
        return (PreciseType) inner;
      }
    } else if (t instanceof PreciseType) {
      return (PreciseType) t;
    }

    throw new RuntimeException("Cannot convert to PreciseType: " + t);
  }

  public int compareTo(PreciseType p) {
    if (mScale == p.mScale) {
      return 0;
    } else if (mScale < p.mScale) {
      return -1;
    } else {
      return 1;
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object coerceValue(Type valType, Object val) {
    if (null == val) {
      return null;
    } else {
      assert valType.isNumeric();
      return parseStringInput(val.toString());
    }
  }
}

