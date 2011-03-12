// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.builtins;

import java.sql.Timestamp;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;

/**
 * Return the current time as a TIMESTAMP.
 */
public class current_timestamp extends ScalarFunc {
  @Override
  public Type getReturnType() {
    return Type.getPrimitive(Type.TypeName.TIMESTAMP);
  }

  @Override
  public Object eval(Object... args) {
    return new Timestamp(System.currentTimeMillis());
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.emptyList();
  }
}
