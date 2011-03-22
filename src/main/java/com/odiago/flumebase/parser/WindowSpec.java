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

package com.odiago.flumebase.parser;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;

/**
 * Defines a window over a range interval.
 */
public class WindowSpec extends Expr {

  /** The range of time over which this window sees. */
  private RangeSpec mRangeSpec;

  public WindowSpec(RangeSpec rangeSpec) {
    mRangeSpec = rangeSpec;
  }

  public RangeSpec getRangeSpec() {
    return mRangeSpec;
  }

  public void setRangeSpec(RangeSpec rangeSpec) {
    mRangeSpec = rangeSpec;
  }

  @Override
  public boolean isConstant() {
    return true;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("WindowSpec\n");
    mRangeSpec.format(sb, depth + 1);
  }

  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    sb.append("WINDOW(");
    sb.append(mRangeSpec.toStringOneLine());
    sb.append(")");
    return sb.toString();
  }

  @Override
  public Type getType(SymbolTable symTab) {
    return getResolvedType();
  }

  @Override
  public Type getResolvedType() {
    return Type.getPrimitive(Type.TypeName.WINDOW);
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    // TODO(aaron): If we allow ORDER BY, PARTITION BY clauses in windows, add
    // the required fields here.
    return Collections.emptyList();
  }

  @Override
  public Object eval(EventWrapper inWrapper) {
    // WindowSpec instances evaluate to themselves; they are already
    // considered constant values in the runtime system.
    return this;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    WindowSpec other = (WindowSpec) otherObj;
    return mRangeSpec.equals(other.mRangeSpec);
  }

  @Override
  public int hashCode() {
    return mRangeSpec.hashCode();
  }
}
