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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

/**
 * Represents a nullable instance of a type (e.g. "INT" vs. "INT NOT NULL").
 */
public class NullableType extends Type {
  
  /** the type name of the non-null type being wrapped. */
  private TypeName mNullableType;

  protected NullableType(TypeName name) {
    super(TypeName.NULLABLE);
    mNullableType = name;
  }

  @Override
  public boolean isNullable() {
    return true;
  }

  @Override
  public boolean isNumeric() {
    switch (mNullableType) {
    case ANY: // Pure null can be any type-class, including numeric.
    case INT:
    case BIGINT:
    case FLOAT:
    case DOUBLE:
      return true;
    default:
      return false;
    }
  }

  @Override
  public boolean isComparable() {
    return isNumeric() || this.equals(Type.getNullable(Type.TypeName.STRING));
  }

  @Override
  /** 
   * @return the TypeName of the non-null type being wrapped.
   */
  public TypeName getPrimitiveTypeName() {
    return mNullableType;
  }

  @Override
  public Schema getAvroSchema() {
    // Our schema is a union of our ordinary type, or null.
    List<Schema> unionTypes = new ArrayList<Schema>();
    unionTypes.add(getAvroSchema(mNullableType));
    unionTypes.add(Schema.create(Schema.Type.NULL));
    return Schema.createUnion(unionTypes);
  }

  @Override
  public Type widen() {
    if (TypeName.INT.equals(mNullableType)) {
      return Type.getNullable(TypeName.BIGINT);
    } else if (TypeName.BIGINT.equals(mNullableType)) {
      return Type.getNullable(TypeName.FLOAT);
    } else if (TypeName.FLOAT.equals(mNullableType)) {
      return Type.getNullable(TypeName.DOUBLE);
    }

    // Cannot widen this type.
    return null;
  }

  @Override
  public String toString() {
    return "" + mNullableType;
  }

  @Override
  public int hashCode() {
    // Return a different position than its basic type counterpart.
    return 3 * super.hashCode() + 7;
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

    NullableType otherType = (NullableType) other;
    if (mNullableType.equals(otherType.mNullableType)) {
      return true;
    }

    return false;
  }
}
