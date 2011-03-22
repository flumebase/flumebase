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

package com.odiago.flumebase.exec;

import java.util.List;

import com.odiago.flumebase.lang.FnType;
import com.odiago.flumebase.lang.Function;
import com.odiago.flumebase.lang.Type;

/**
 * A symbol representing a callable function.
 */
public class FnSymbol extends Symbol {

  /** The types of all the arguments. */
  private final List<Type> mArgTypes;

  /** The return type of the function. */
  private final Type mRetType;

  /** The function instance itself. */
  private final Function mFunc;

  public FnSymbol(String name, Function func, Type retType, List<Type> argTypes) {
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

  public Function getFuncInstance() {
    return mFunc;
  }

  @Override
  public Symbol withName(String name) {
    return new FnSymbol(name, mFunc, mRetType, mArgTypes);
  }
}
