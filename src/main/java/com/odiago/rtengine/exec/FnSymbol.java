// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.List;

import com.odiago.rtengine.lang.FnType;
import com.odiago.rtengine.lang.ScalarFunc;
import com.odiago.rtengine.lang.Type;

/**
 * A symbol representing a callable function.
 */
public class FnSymbol extends Symbol {

  /** The types of all the arguments. */
  private final List<Type> mArgTypes;

  /** The return type of the function. */
  private final Type mRetType;

  /** The function instance itself. */
  private final ScalarFunc mFunc;

  public FnSymbol(String name, ScalarFunc func, Type retType, List<Type> argTypes) {
    super(name, new FnType(retType, argTypes));
    mFunc = func;
    mRetType = retType;
    mArgTypes = argTypes;
  }

  public List<Type> getArgumentTypes() {
    return mArgTypes;
  }

  public Type getReturnType() {
    return mRetType;
  }

  public ScalarFunc getFuncInstance() {
    return mFunc;
  }

  @Override
  public Symbol withName(String name) {
    return new FnSymbol(name, mFunc, mRetType, mArgTypes);
  }
}
