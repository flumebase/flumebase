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

package com.odiago.flumebase.lang;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

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

  @Test
  public void testNullable() {
    assertTrue(Type.getPrimitive(Type.TypeName.INT).isPrimitive());
    assertFalse(Type.getPrimitive(Type.TypeName.INT).isNullable());

    // nullable types are isPrimitive() and isNullable().
    assertTrue(Type.getNullable(Type.TypeName.INT).isPrimitive());
    assertTrue(Type.getNullable(Type.TypeName.INT).isNullable());
  }

  @Test
  public void testPromotion() {
    // Reflexivity.
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));
    assertTrue(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));
    assertTrue(Type.getPrimitive(Type.TypeName.TIMESTAMP).promotesTo(
        Type.getPrimitive(Type.TypeName.TIMESTAMP)));

    // INT promotion via widening.
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.BIGINT)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.FLOAT)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.DOUBLE)));

    assertTrue(Type.getPrimitive(Type.TypeName.BIGINT).promotesTo(
        Type.getPrimitive(Type.TypeName.DOUBLE)));

    // INT to STRING.
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));

    // INT to NULLABLE(INT), NULLABLE(FLOAT), NULLABLE(STRING)
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.INT)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.FLOAT)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));

    // BINARY type
    assertTrue(Type.getPrimitive(Type.TypeName.BINARY).promotesTo(
        Type.getNullable(Type.TypeName.BINARY)));
    assertTrue(Type.getPrimitive(Type.TypeName.BINARY).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));

    assertFalse(Type.getPrimitive(Type.TypeName.BINARY).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));
    assertFalse(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.BINARY)));
    assertFalse(Type.getPrimitive(Type.TypeName.BINARY).promotesTo(
        Type.getNullable(Type.TypeName.INT)));
  
    // STRING to NULLABLE(STRING).
    assertTrue(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));
    
    // NULLABLE(ANY) to NULLABLE(INT), NULLABLE(STRING), NULLABLE(TIMESTAMP).
    assertTrue(Type.getNullable(Type.TypeName.ANY).promotesTo(
        Type.getNullable(Type.TypeName.INT)));
    assertTrue(Type.getNullable(Type.TypeName.ANY).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));
    assertTrue(Type.getNullable(Type.TypeName.ANY).promotesTo(
        Type.getNullable(Type.TypeName.TIMESTAMP)));

    // NULLABLE(X) -> NULLABLE(STRING).
    assertTrue(Type.getNullable(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));
    assertTrue(Type.getNullable(Type.TypeName.TIMESTAMP).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));
    assertTrue(Type.getNullable(Type.TypeName.STRING).promotesTo(
        Type.getNullable(Type.TypeName.STRING)));


    // Test typeclasses:
    assertTrue(Type.getNullable(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_NUMERIC)));

    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_ANY)));
    assertTrue(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_ANY)));
    assertFalse(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_NUMERIC)));

    // And now check that the above are the *only* rules we admit.

    // STRING to INT fails.
    assertFalse(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));

    // numeric narrowing fails. 
    assertFalse(Type.getPrimitive(Type.TypeName.BIGINT).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));
    assertFalse(Type.getPrimitive(Type.TypeName.DOUBLE).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));
    assertFalse(Type.getPrimitive(Type.TypeName.DOUBLE).promotesTo(
        Type.getPrimitive(Type.TypeName.FLOAT)));

    // NULLABLE(X) to X fails
    assertFalse(Type.getNullable(Type.TypeName.DOUBLE).promotesTo(
        Type.getPrimitive(Type.TypeName.DOUBLE)));

    // NULLABLE(X) to Y for X -> Y fails.
    assertFalse(Type.getNullable(Type.TypeName.FLOAT).promotesTo(
        Type.getPrimitive(Type.TypeName.DOUBLE)));

    assertFalse(Type.getNullable(Type.TypeName.FLOAT).promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));

    // NULLABLE(ANY) to X fails.
    assertFalse(Type.getNullable(Type.TypeName.ANY).promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));
    assertFalse(Type.getNullable(Type.TypeName.ANY).promotesTo(
        Type.getPrimitive(Type.TypeName.INT)));


    // STREAM does not promote to STRING.
    assertFalse(StreamType.getEmptyStreamType().promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));


    // LIST(INT) promotes to LIST(BIGINT)
    assertTrue(new ListType(Type.getPrimitive(Type.TypeName.INT)).promotesTo(
        new ListType(Type.getPrimitive(Type.TypeName.BIGINT))));

    // Not vice versa
    assertFalse(new ListType(Type.getPrimitive(Type.TypeName.BIGINT)).promotesTo(
        new ListType(Type.getPrimitive(Type.TypeName.INT))));

    // LIST(INT NOT NULL) promotes to LIST(INT)
    assertTrue(new ListType(Type.getPrimitive(Type.TypeName.INT)).promotesTo(
        new ListType(Type.getNullable(Type.TypeName.INT))));

    // Lists can't turn into strings.
    assertFalse(new ListType(Type.getPrimitive(Type.TypeName.INT)).promotesTo(
        Type.getPrimitive(Type.TypeName.STRING)));
  }

  // Tests for promotion to/of PRECISE types.
  @Test
  public void testPrecise() {
    // String doesn't promote to Precise
    assertFalse(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        new PreciseType(0)));

    // But the other way around works.
    assertTrue(new PreciseType(0).promotesTo(Type.getPrimitive(Type.TypeName.STRING)));

    // Int types promote to PRECISE(0).
    assertTrue(Type.getPrimitive(Type.TypeName.BIGINT).promotesTo(new PreciseType(0)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(new PreciseType(0)));

    // Floats promote to PRECISE(24) and above.
    assertTrue(Type.getPrimitive(Type.TypeName.FLOAT).promotesTo(new PreciseType(24)));
    assertTrue(Type.getPrimitive(Type.TypeName.FLOAT).promotesTo(new PreciseType(80)));

    // but not this.
    assertFalse(Type.getPrimitive(Type.TypeName.FLOAT).promotesTo(new PreciseType(6)));

    // Instead, however, they will meet at PRECISE(24):
    assertEquals(new PreciseType(24),
        Type.meet(Type.getPrimitive(Type.TypeName.FLOAT), new PreciseType(6)));
  }

  // Tests for the TYPECLASS_COMPARABLE typeclass.
  @Test
  public void testComparable() {
    // test STRING NOT NULL.
    assertTrue(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_COMPARABLE)));
    assertTrue(Type.getPrimitive(Type.TypeName.STRING).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE)));

    // nullable Strings...
    assertTrue(Type.getNullable(Type.TypeName.STRING).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE)));
    assertFalse(Type.getNullable(Type.TypeName.STRING).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_COMPARABLE)));

    // Check typeclass promotion.
    assertTrue(Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_ANY)));
    assertTrue(Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE)));
    assertFalse(Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_NUMERIC)));

    // Integers
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getNullable(Type.TypeName.TYPECLASS_COMPARABLE)));
    assertTrue(Type.getPrimitive(Type.TypeName.INT).promotesTo(
        Type.getPrimitive(Type.TypeName.TYPECLASS_COMPARABLE)));

  }

}
