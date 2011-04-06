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

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines a static or runtime type within the rtsql
 * language. Complex types may be defined in subclasses of this
 * type to hold additional information.
 *
 * <p>The equals() and hashCode() methods on types work as expected.
 * </p>
 */
public class Type {

  private static final Logger LOG = LoggerFactory.getLogger(Type.class.getName());

  /**
   * Every type in rtsql has a name specified here.
   */
  public enum TypeName {
    STREAM, // Collection (record) of named primitive or nullable types.
    BOOLEAN,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    PRECISE, // A numeric type which allows the user to specify an arbitrary
             // degree of precision.
    STRING,
    TIMESTAMP,
    TIMESPAN,
    NULLABLE, // nullable instance of a primitive type (int, bigint, etc).
    FLOW, // An executing flow.
    ANY, // 'null' constant can be cast to any type. Only valid inside NULLABLE.
         // This represents the "bottom" of the promotesTo type lattice.
    SCALARFUNC, // Callable scalar function (FnType).
    TYPECLASS_NUMERIC, // Typeclass representing all numeric types.
    TYPECLASS_COMPARABLE, // Typeclass representing all comparable types.
    TYPECLASS_ANY,     // Typeclass representing all types. This is the "top" of
                       // the promotesTo lattice. (nothing promotesTo ANY, but
                       // everything promotesTo TYPECLASS_ANY.)
    UNIVERSAL, // Represents an unbound type variable in a UniversalType instance.
    WINDOW, // A bounded window of time which collects records.
  };

  
  /** The basic type name for this type. */
  private TypeName mTypeName;

  /** Do not allow public instantiation of Type instances; primitive
   * types, etc, have singleton instances grabbed via a getter method.
   */
  protected Type(TypeName name) {
    mTypeName = name;
  }

  /**
   * Map containing primitive type instances by TypeName. Used by getPrimitive().
   */
  private static final Map<TypeName, Type> PRIMITIVE_TYPES;

  /**
   * Map containing nullable versions of the primitive types instance, by TypeName.
   * Used by getNullable().
   */
  private static final Map<TypeName, NullableType> NULLABLE_TYPES;

  static {
    PRIMITIVE_TYPES = new HashMap<TypeName, Type>();
    PRIMITIVE_TYPES.put(TypeName.BOOLEAN, new Type(TypeName.BOOLEAN));
    PRIMITIVE_TYPES.put(TypeName.INT, new Type(TypeName.INT));
    PRIMITIVE_TYPES.put(TypeName.BIGINT, new Type(TypeName.BIGINT));
    PRIMITIVE_TYPES.put(TypeName.FLOAT, new Type(TypeName.FLOAT));
    PRIMITIVE_TYPES.put(TypeName.DOUBLE, new Type(TypeName.DOUBLE));
    PRIMITIVE_TYPES.put(TypeName.STRING, new Type(TypeName.STRING));
    PRIMITIVE_TYPES.put(TypeName.TIMESTAMP, new Type(TypeName.TIMESTAMP));
    PRIMITIVE_TYPES.put(TypeName.TIMESPAN, new Type(TypeName.TIMESPAN));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_NUMERIC, new Type(TypeName.TYPECLASS_NUMERIC));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_COMPARABLE, new Type(TypeName.TYPECLASS_COMPARABLE));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_ANY, new Type(TypeName.TYPECLASS_ANY));
    // Window is in primitive types, but is not allowed to be nullable.
    PRIMITIVE_TYPES.put(TypeName.WINDOW, new Type(TypeName.WINDOW));

    NULLABLE_TYPES = new HashMap<TypeName, NullableType>();
    NULLABLE_TYPES.put(TypeName.BOOLEAN, new NullableType(TypeName.BOOLEAN));
    NULLABLE_TYPES.put(TypeName.INT, new NullableType(TypeName.INT));
    NULLABLE_TYPES.put(TypeName.BIGINT, new NullableType(TypeName.BIGINT));
    NULLABLE_TYPES.put(TypeName.FLOAT, new NullableType(TypeName.FLOAT));
    NULLABLE_TYPES.put(TypeName.DOUBLE, new NullableType(TypeName.DOUBLE));
    NULLABLE_TYPES.put(TypeName.STRING, new NullableType(TypeName.STRING));
    NULLABLE_TYPES.put(TypeName.TIMESTAMP, new NullableType(TypeName.TIMESTAMP));
    NULLABLE_TYPES.put(TypeName.TIMESPAN, new NullableType(TypeName.TIMESPAN));
    NULLABLE_TYPES.put(TypeName.ANY, new NullableType(TypeName.ANY));
    NULLABLE_TYPES.put(TypeName.TYPECLASS_NUMERIC, new NullableType(TypeName.TYPECLASS_NUMERIC));
    NULLABLE_TYPES.put(TypeName.TYPECLASS_COMPARABLE,
        new NullableType(TypeName.TYPECLASS_COMPARABLE));
    NULLABLE_TYPES.put(TypeName.TYPECLASS_ANY, new NullableType(TypeName.TYPECLASS_ANY));
  }

  /**
   * @return the object that defines a primitive type with the specified
   * TypeName.
   */
  public static Type getPrimitive(TypeName name) {
    return PRIMITIVE_TYPES.get(name);
  }

  /**
   * @return the object that defines a nullable primitive type with the specified
   * TypeName.
   */
  public static Type getNullable(TypeName name) {
    return NULLABLE_TYPES.get(name);
  }

  public TypeName getTypeName() {
    return mTypeName;
  }

  /**
   * If this is a primitive type, return the TypeName it represents.
   */
  public TypeName getPrimitiveTypeName() {
    if (!isPrimitive()) {
      return null;
    }
    return mTypeName;
  }

  /**
   * @return a type object describing the nullable form of this type.
   */
  public NullableType asNullable() {
    // If we've got a single representation instance, use that.
    NullableType premade = NULLABLE_TYPES.get(mTypeName);
    if (null != premade) {
      return null;
    }

    return new NullableType(this);
  }

  /**
   * @return true if a null value may be used in this type.
   */
  public boolean isNullable() {
    return false;
  }

  /** @return true if this is a primitive type (Non-recursive) */
  public boolean isPrimitive() {
    return true;
  }

  /** @return true if this is a numeric type. */
  public boolean isNumeric() {
    switch (mTypeName) {
    case INT:
    case BIGINT:
    case FLOAT:
    case DOUBLE:
    case PRECISE:
    case TYPECLASS_NUMERIC:
      return true;
    default:
      return false;
    }
  }

  /** @return true if there is an ordering over values of this type (i.e., it
   * supports operators &gt;, &lt;, &gt;=, &lt;=).
   */
  public boolean isComparable() {
    return isNumeric() || this.equals(Type.getPrimitive(TypeName.BOOLEAN))
        || this.equals(Type.getPrimitive(TypeName.STRING))
        || this.equals(Type.getPrimitive(TypeName.TYPECLASS_COMPARABLE));
  }

  /** @return an Avro schema describing this type. */
  public Schema getAvroSchema() {
    return getAvroSchema(mTypeName);
  }

  /**
   * @return true if a value of this type can be represented in the form of 'other'.
   * For example, INT promotesTo NULLABLE(INT).
   *
   * <p>The general rules are as follows:</p>
   * <ul>
   *   <li>X promotesTo X (reflexivity)</li>
   *   <li>X promotesTo Y &amp;&amp; Y promotesTo Z =&gt; X promotesTo Z (transitivity)</li>
   *   <li>X promotesTo NULLABLE(Y) for any scalar X if X promotesTo Y</li>
   *   <li>NULLABLE(ANY) promotesTo NULLABLE(X) for any X</li>
   *   <li>X promotesTo STRING for any scalar X</li>
   *   <li>NULLABLE(X) promotesTo NULLABLE(STRING) for any scalar X</li>
   *   <li>INT promotesTo BIGINT</li>
   *   <li>BIGINT promotesTo FLOAT</li>
   *   <li>FLOAT promotesTo DOUBLE</li>
   *   <li>All numeric types promotesTo TYPECLASS_NUMERIC.</li>
   *   <li>TYPECLASS_NUMERIC, STRING, and BOOLEAN promotesTo TYPECLASS_COMPARABLE.</li>
   *   <li>Anything promotesTo TYPECLASS_ANY (including other typeclasses).</li>
   *   <li>X promotesTo UniversalType(C1, C2...Cn) iff X promotesTo all constraints Ci</li>
   *
   *   <li>(TODO: unimplemented)
   *       FLOW(t1, ..., tn) promotesTo FLOW(t'1, ..., t'n) iff t_i promotesTo t'_i.</li>
   * </ul>
   */
  public boolean promotesTo(Type other) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking: " + this + " promotesTo " + other);
    }

    if (null == other) {
      return false; // invalid input.
    } else if (this.equals(other)) {
      return true; // reflexivity rule.
    } else if (!isNullable() && other.equals(Type.getPrimitive(TypeName.TYPECLASS_ANY))) {
      // Any non-null type promotes to non-null TYPECLASS_ANY.
      return true;
    } else if (isNullable() && other.equals(Type.getNullable(TypeName.TYPECLASS_ANY))) {
      // Any nullable type promotes to nullable TYPECLASS_ANY.
      return true;
    } else if (isComparable() && !isNullable()
        && other.equals(Type.getPrimitive(TypeName.TYPECLASS_COMPARABLE))) {
      // Any comparable non-null type promotes to non-null TYPECLASS_COMPARABLE.
      return true;
    } else if (isComparable() && other.equals(Type.getNullable(TypeName.TYPECLASS_COMPARABLE))) {
      // Any comparable type promotes to nullable TYPECLASS_COMPARABLE.
      return true;
    } else if (other instanceof UniversalType) {
      // We can promote to a constrained universal type if we can promote
      // to all constraints of that type.
      for (Type constraint : ((UniversalType) other).getConstraints()) {
        if (!promotesTo(constraint)) {
          return false;
        }
      }

      return true;
    } else if (isPrimitive() && other.isNullable()) {
      if (isNullable()) {
        NullableType nullableThis = (NullableType) this;
        NullableType nullableOther = (NullableType) other;

        TypeName myTypeName = nullableThis.getPrimitiveTypeName();
        TypeName otherTypeName = nullableOther.getPrimitiveTypeName();
        if (TypeName.ANY.equals(myTypeName)) {
          // NULLABLE(ANY) promotesTo any nullable other type.
          return true;
        } else if (Type.getPrimitive(myTypeName).promotesTo(Type.getPrimitive(otherTypeName))) {
          // NULLABLE (X) promotesTo NULLABLE (Y) if X promotesTo Y.
          return true;
        } else {
          return false;
        }
      } else {
        // X promotesTo NULLABLE(Y) if X promotesTo Y.
        NullableType nullableOther = (NullableType) other;
        Type nonNullVer = Type.getPrimitive(nullableOther.getPrimitiveTypeName());
        return promotesTo(nonNullVer);
      }
    } else if (isPrimitive() && !isNullable() && other.isPrimitive()) {
      TypeName otherName = other.getTypeName();
      if (TypeName.STRING.equals(otherName)) {
        // any primitive type promotes to STRING.
        return true;
      } else if (numericPromotesTo(mTypeName, otherName)) {
        // our numeric type promotes to the other type.
        return true;
      } else {
        // Transitive widening case: If our numeric type can be widened, see
        // if that promotes to the target type (recursively).
        Type widerPrimitive = widen();
        if (null != widerPrimitive) {
          return widerPrimitive.promotesTo(other);
        } else {
          return false; // can't widen.
        }
      }
    }

    return false;
  }

  /**
   * @return true if smaller is a numeric TypeName, and smaller promotesTo larger.
   */
  private boolean numericPromotesTo(TypeName smaller, TypeName larger) {
    return
        (TypeName.INT.equals(smaller) &&
            (TypeName.BIGINT.equals(larger)
            || TypeName.FLOAT.equals(larger)
            || TypeName.DOUBLE.equals(larger)
            || TypeName.TYPECLASS_NUMERIC.equals(larger)))
        || (TypeName.BIGINT.equals(smaller) &&
            (TypeName.FLOAT.equals(larger)
            || TypeName.DOUBLE.equals(larger)
            || TypeName.TYPECLASS_NUMERIC.equals(larger)))
        || (TypeName.FLOAT.equals(smaller) &&
            (TypeName.DOUBLE.equals(larger)
            || TypeName.TYPECLASS_NUMERIC.equals(larger)))
        || smaller.equals(larger);
  }

  /**
   * For numeric types, returns the next more permissive numeric type in the tower.
   * <p>INT -&gt; BIGINT -&gt; FLOAT -&gt; DOUBLE.</p>
   * <p>widen(DOUBLE) returns null.</p>
   * <p>X widensTo Y =&gt; NULLABLE(X) widensTo NULLABLE(Y).
   * (Handled in NullableType.widen())</p>
   */
  public Type widen() {
    if (TypeName.INT.equals(mTypeName)) {
      return Type.getPrimitive(TypeName.BIGINT);
    } else if (TypeName.BIGINT.equals(mTypeName)) {
      return Type.getPrimitive(TypeName.FLOAT);
    } else if (TypeName.FLOAT.equals(mTypeName)) {
      return Type.getPrimitive(TypeName.DOUBLE);
    }

    // Cannot widen this type.
    return null;
  }

  /** @return an Avro schema describing the specified TypeName. */
  protected Schema getAvroSchema(TypeName typeName) { 
    switch (typeName) {
    case BOOLEAN:
      return Schema.create(Schema.Type.BOOLEAN);
    case INT:
      return Schema.create(Schema.Type.INT);
    case BIGINT:
      return Schema.create(Schema.Type.LONG);
    case FLOAT:
      return Schema.create(Schema.Type.FLOAT);
    case DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING:
      return Schema.create(Schema.Type.STRING);
    case TIMESPAN:
      return TimeSpan.SCHEMA$;
    case TIMESTAMP:
      return Timestamp.SCHEMA$;
    default:
      LOG.error("Cannot create avro schema for type: " + toString());
      return null;
    }
  }

  public String toString(boolean isNullable) {
    if (isNullable) {
      return mTypeName.name();
    } else {
      return mTypeName.name() + " NOT NULL";
    }
  }

  @Override
  public String toString() {
    return toString(false);
  }

  @Override
  public int hashCode() {
    return mTypeName.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other == null) {
      return false;
    } else if (!other.getClass().equals(getClass())) {
      return false;
    }

    Type otherType = (Type) other;
    if (mTypeName.equals(otherType.mTypeName)) {
      return true;
    }

    return false;
  }
}
