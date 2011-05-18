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

package com.odiago.flumebase.exec.builtins;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;

import com.odiago.flumebase.lang.ListType;
import com.odiago.flumebase.lang.NullableType;
import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return the concatenation of a set of lists.
 * Return null if no arguments are given.
 */
public class concat extends ScalarFunc {
  private UniversalType mArgType;

  public concat() {
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_ANY));
  }

  @Override
  public Type getReturnType() {
    return new NullableType(new ListType(mArgType));
  }

  @Override
  public Object eval(EventWrapper event, Object... args) {
    if (null == args || args.length == 0) {
      return null;
    }
    
    List<Object> out = new ArrayList<Object>();
    for (Object arg : args) {
      List<Object> in = (List<Object>) arg;
      out.addAll(in);
    }

    return out;
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.emptyList();
  }

  @Override
  public List<Type> getVarArgTypes() {
    return Collections.singletonList((Type) new NullableType(new ListType(mArgType)));
  }
}
