// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

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
