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

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;

import com.odiago.flumebase.lang.ListType;
import com.odiago.flumebase.lang.NullableType;
import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return the number of elements in a list.
 * Return null if the list is null.
 */
public class size extends ScalarFunc {
  private UniversalType mArgType;

  public size() {
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_ANY));
  }

  @Override
  public Type getReturnType() {
    return Type.getNullable(Type.TypeName.INT);
  }

  @Override
  public Object eval(EventWrapper event, Object... args) {
    List<Object> lst = (List<Object>) args[0];
   
    if (null == lst) {
      return null;
    } else {
      return Integer.valueOf(lst.size());
    }
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList((Type) new NullableType(new ListType(mArgType)));
  }
}
