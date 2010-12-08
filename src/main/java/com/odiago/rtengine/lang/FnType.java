// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.List;

import com.odiago.rtengine.util.StringUtils;

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
}
