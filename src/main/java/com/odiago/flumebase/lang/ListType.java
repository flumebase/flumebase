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

import java.util.Map;

import org.apache.avro.Schema;

/**
 * Represents a LIST&lt;t&gt; type.
 *
 * <p>This non-scalar type supports an arbitrary-length list of values
 * of the same type.</p>
 */
public class ListType extends Type implements Comparable<ListType> {

  /** Type for each element in the list */
  private Type mElementType;

  public ListType(Type elemType) {
    super(TypeName.LIST);
    this.mElementType = elemType;

    assert null != elemType;
  }

  public Type getElementType() {
    return mElementType;
  }

  @Override
  public String toString(boolean isNullable) {
    if (isNullable) {
      return "LIST<" + mElementType + ">";
    } else {
      return "LIST<" + mElementType + "> NOT NULL";
    }
  }

  @Override
  public Schema getAvroSchema() {
    return Schema.createArray(mElementType.getAvroSchema());
  }

  @Override
  public int hashCode() {
    return Type.TypeName.LIST.hashCode() * 7 + 11 * mElementType.hashCode();
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

    ListType otherList = (ListType) other;
    return mElementType.equals(otherList.mElementType);
  }

  @Override
  public int compareTo(ListType lstType) {
    return mElementType.getPrimitiveTypeName().meetCompareLevel(lstType.getPrimitiveTypeName());
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public boolean isConcrete() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public Type replaceUniversal(Map<Type, Type> universalMapping) throws TypeCheckException {
    return new ListType(mElementType.replaceUniversal(universalMapping));
  }
}


