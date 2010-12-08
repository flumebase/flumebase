// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.lang.ScalarFunc;
import com.odiago.rtengine.lang.Type;

/**
 * Return arg^2.
 */
public class square_int extends ScalarFunc {
  @Override
  public Type getReturnType() {
    return Type.getNullable(Type.TypeName.INT);
  }

  @Override
  public Object eval(Object... args) {
    Integer arg0 = (Integer) args[0];
    if (null == arg0) {
      return null;
    } else {
      return Integer.valueOf(arg0.intValue() * arg0.intValue());
    }
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.singletonList(Type.getNullable(Type.TypeName.INT));
  }
}
