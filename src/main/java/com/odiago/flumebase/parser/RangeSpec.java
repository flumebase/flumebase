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

import java.io.IOException;

import java.lang.String;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EmptyEventWrapper;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.TimeSpan;
import com.odiago.flumebase.lang.Type;

/**
 * Defines a range of time.
 */
public class RangeSpec extends Expr {

  /** Defines how far back we look in time. */
  private Expr mPrevSize;
  private TimeWidth mPrevScale;

  /** Defines how far to look ahead. */ 
  private Expr mAfterSize;
  private TimeWidth mAfterScale;

  public RangeSpec(Expr prevSize, TimeWidth prevScale) {
    this(prevSize, prevScale, new ConstExpr(
        Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(0)), TimeWidth.BaseUnits);
  }

  public RangeSpec(Expr prevSize, TimeWidth prevScale, Expr afterSize, TimeWidth afterScale) {
    mPrevSize = prevSize;
    mPrevScale = prevScale;

    mAfterSize = afterSize;
    mAfterScale = afterScale;
  }

  public Expr getPrevSize() {
    return mPrevSize;
  }

  public void setPrevSize(Expr prevSize) {
    mPrevSize = prevSize;
  }

  public TimeWidth getPrevScale() {
    return mPrevScale;
  }

  public Expr getAfterSize() {
    return mAfterSize;
  }

  public void setAfterSize(Expr afterSize) {
    mAfterSize = afterSize;
  }

  public TimeWidth getAfterScale() {
    return mAfterScale;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("RangeSpec\n");
    pad(sb, depth + 1);
    sb.append("from: (");
    sb.append(mPrevScale);
    sb.append(")\n");
    mPrevSize.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("to: (");
    sb.append(mAfterScale);
    sb.append(")\n");
    mAfterSize.format(sb, depth + 2);
  }

  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    sb.append("RANGE(From=");
    try {
      sb.append(mPrevSize.eval(new EmptyEventWrapper()));
    } catch (IOException ioe) {
      sb.append("???");
    }
    sb.append(" ");
    sb.append(mPrevScale);
    sb.append(", To=");
    try {
      sb.append(mAfterSize.eval(new EmptyEventWrapper()));
    } catch (IOException ioe) {
      sb.append("???");
    }
    sb.append(" ");
    sb.append(mAfterScale);
    sb.append(")");
    return sb.toString();
  }

  @Override
  public Type getType(SymbolTable symTab) {
    return getResolvedType();
  }

  @Override
  protected Type getResolvedType() {
    return Type.getPrimitive(Type.TypeName.TIMESPAN);
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    return Collections.emptyList();
  }

  /**
   * Return a relative TimeSpan interval describing the number of milliseconds
   * prior which this timespan encompasses.
   */
  @Override
  public Object eval(EventWrapper inWrapper) throws IOException {
    Number lowerBound = (Number) mPrevSize.eval(inWrapper);
    long lowerBoundMillis = -1L * lowerBound.longValue() * mPrevScale.getMultiplier();

    Number upperBound = (Number) mAfterSize.eval(inWrapper);
    long upperBoundMillis = upperBound.longValue() * mAfterScale.getMultiplier();

    return new TimeSpan(lowerBoundMillis, upperBoundMillis, true);
  }

  @Override
  public boolean isConstant() {
    // RangeSpec instances must be constant; subexpressions are not
    // allowed to be non constant.
    return true;
  }
}
