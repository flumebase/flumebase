// (c) Copyright 2010 Odiago, Inc.

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
