// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.lang.String;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.EmptyEventWrapper;
import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.TimeSpan;
import com.odiago.rtengine.lang.Type;

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

  public TimeWidth getPrevScale() {
    return mPrevScale;
  }

  public Expr getAfterSize() {
    return mAfterSize;
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
