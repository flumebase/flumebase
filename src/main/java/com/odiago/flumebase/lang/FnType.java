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

import java.util.List;

import com.odiago.flumebase.util.StringUtils;

/**
 * A type representing a callable function.
 */
public class FnType extends Type {
  // The return type of the function.
  private Type mRetType;

  // The argument types to the function.
  private List<Type> mArgTypes;

  public FnType(Type retType, List<Type> argTypes) {
    super(TypeName.SCALARFUNC);
    mRetType = retType;
    mArgTypes = argTypes;
  }

  public Type getReturnType() {
    return mRetType;
  }

  public List<Type> getArgumentTypes() {
    return mArgTypes;
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    StringUtils.formatList(sb, mArgTypes);
    sb.append(") -> ");
    sb.append(mRetType);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    int hash = mRetType.hashCode();
    for (Type argT : mArgTypes) {
      hash ^= argT.hashCode();
    }

    return hash;
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

    FnType otherType = (FnType) other;
    if (!mRetType.equals(otherType.mRetType)) {
      return false;
    }

    if (mArgTypes.size() != otherType.mArgTypes.size()) {
      return false;
    }

    for (int i = 0; i < mArgTypes.size(); i++) {
      if (!mArgTypes.get(i).equals(otherType.mArgTypes.get(i))) {
        return false;
      }
    }

    return true;
  }
}
