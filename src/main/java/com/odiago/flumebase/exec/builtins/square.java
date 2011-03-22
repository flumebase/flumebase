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

import com.odiago.flumebase.lang.EvalException;
import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.UniversalType;

/**
 * Return arg^2.
 */
public class square extends ScalarFunc {
  private UniversalType mArgType;

  public square() {
    mArgType = new UniversalType("'a");
    mArgType.addConstraint(Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC));
  }

  @Override
  public Object eval(Object... args) throws EvalException {
    Number arg0 = (Number) args[0];
    if (null == arg0) {
      return null;
    } else if (arg0 instanceof Integer) {
      int i = arg0.intValue();
      return Integer.valueOf(i * i);
    } else if (arg0 instanceof Long) {
      long ln = arg0.longValue();
      return Long.valueOf(ln * ln);
    } else if (arg0 instanceof Float) {
      float f = arg0.floatValue();
      return Float.valueOf(f * f);
    } else if (arg0 instanceof Double) {
      double d = arg0.doubleValue();
      return Double.valueOf(d * d);
    } else {
      throw new EvalException("Cannot square value of type : " + arg0.getClass().getName());
    }
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList((Type) mArgType);
  }

  @Override
  public Type getReturnType() {
    // Return type is the same as our argument type.
    return mArgType;
  }

  @Override
  public boolean autoPromoteArguments() {
    return false;
  }
}
