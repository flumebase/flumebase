// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestType {

  @Test
  public void testEquality() {
    assertTrue(Type.getPrimitive(Type.TypeName.INT).equals(Type.getPrimitive(Type.TypeName.INT)));
    assertFalse(Type.getPrimitive(Type.TypeName.INT).equals(
        Type.getPrimitive(Type.TypeName.STRING)));

    assertTrue(Type.getNullable(Type.TypeName.INT).equals(Type.getNullable(Type.TypeName.INT)));
    assertFalse(Type.getNullable(Type.TypeName.INT).equals(Type.getPrimitive(Type.TypeName.INT)));
    assertFalse(Type.getNullable(Type.TypeName.INT).equals(Type.getNullable(Type.TypeName.STRING)));
  }
}
