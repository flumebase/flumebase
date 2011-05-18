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

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.builtins.bin2str;

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
  public enum TypeName implements Comparable<TypeName> {
    BOOLEAN(1),
    INT(1),
    BIGINT(2),
    FLOAT(3),
    DOUBLE(4),
    PRECISE(5), // A numeric type which allows the user to specify an arbitrary
                // degree of precision.
    STRING(8),
    BINARY(6),
    TIMESTAMP(6),
    TIMESPAN(7),
    ANY(0), // 'null' constant can be cast to any type. Only valid inside NULLABLE or LIST.
            // This represents the "bottom" of the promotesTo type lattice.
    NULLABLE, // nullable instance of a primitive type (int, bigint, etc).
    STREAM, // Collection (record) of named primitive or nullable types.
    FLOW, // An executing flow.
    SCALARFUNC, // Callable scalar function (FnType).
    TYPECLASS_NUMERIC(7), // Typeclass representing all numeric types.
    TYPECLASS_COMPARABLE(8), // Typeclass representing all comparable types.
    TYPECLASS_ANY(9),     // Typeclass representing all types. This is the "top" of
                          // the promotesTo lattice. (nothing promotesTo ANY, but
                          // everything promotesTo TYPECLASS_ANY.)
    UNIVERSAL, // Represents an unbound type variable in a UniversalType instance.
               // (Note that UniversalType.getPrimitiveTypeName() returns null;
               // this is not bound to any objects in practice.
               // Use 't instanceof UniversalType' to check the sort of cast you can
               // make. TODO: This is probably a bug.) 
    WINDOW, // A bounded window of time which collects records.
    LIST,   // Complex type: a list of scalar values, all of the same type.
    ;


    /**
     * Defines how "high up" in the lattice elements are. Used to quickly sort
     * non-PRECISE NUMERIC and other primitive types.
     */
    private final int mMeetLevel;

    private TypeName(int meetLevel) {
      mMeetLevel = meetLevel;
    }

    private TypeName() {
      mMeetLevel = -1; // "Invalid" meet-level for items that aren't in the "clean" lattice.
    }

    /**
     * @return an integer specifying at what "level" of the ordering a type is.
     * if t1.level &lt; t2.level, meet(t1, t2) MAY be t2. It WILL NEVER be t1.
     * if t1.level = t2.level, meet(t1, t2) is undefined.
     */
    public int meetLevel() {
      return mMeetLevel;
    }
    
    /**
     * @return a compareTo()-like value describing whether this type is lower on
     * the lattice than t, based on the meetLevel.
     */
    int meetCompareLevel(TypeName t) {
      if (mMeetLevel < t.mMeetLevel) {
        return -1;
      } else if (mMeetLevel == t.mMeetLevel) {
        return 0;
      } else {
        return 1;
      }
    }
  };

  protected static final bin2str BIN2STR_FN; // For coercing binary -> string
  static {
    BIN2STR_FN = new bin2str();
  }
  
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
    PRIMITIVE_TYPES.put(TypeName.BINARY, new Type(TypeName.BINARY));
    PRIMITIVE_TYPES.put(TypeName.TIMESTAMP, new Type(TypeName.TIMESTAMP));
    PRIMITIVE_TYPES.put(TypeName.TIMESPAN, new Type(TypeName.TIMESPAN));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_NUMERIC, new Type(TypeName.TYPECLASS_NUMERIC));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_COMPARABLE, new Type(TypeName.TYPECLASS_COMPARABLE));
    PRIMITIVE_TYPES.put(TypeName.TYPECLASS_ANY, new Type(TypeName.TYPECLASS_ANY));
    // Window is in primitive types, but is not allowed to be nullable.
    PRIMITIVE_TYPES.put(TypeName.WINDOW, new Type(TypeName.WINDOW));
    PRIMITIVE_TYPES.put(TypeName.ANY, new Type(TypeName.ANY));

    NULLABLE_TYPES = new HashMap<TypeName, NullableType>();
    NULLABLE_TYPES.put(TypeName.BOOLEAN, new NullableType(TypeName.BOOLEAN));
    NULLABLE_TYPES.put(TypeName.INT, new NullableType(TypeName.INT));
    NULLABLE_TYPES.put(TypeName.BIGINT, new NullableType(TypeName.BIGINT));
    NULLABLE_TYPES.put(TypeName.FLOAT, new NullableType(TypeName.FLOAT));
    NULLABLE_TYPES.put(TypeName.DOUBLE, new NullableType(TypeName.DOUBLE));
    NULLABLE_TYPES.put(TypeName.STRING, new NullableType(TypeName.STRING));
    NULLABLE_TYPES.put(TypeName.BINARY, new NullableType(TypeName.BINARY));
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
      return premade;
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

  /** @return true if this is a concrete type that can actually be instantiated
   * with values (i.e., not a typeclass). */
  public boolean isConcrete() {
    return isScalar();
  }

  /** @return true if this is a concrete, scalar-valued type (a concrete type
   * that can hold one value.
   */
  public boolean isScalar() {
    switch (mTypeName) {
    case BOOLEAN:
    case INT:
    case BIGINT:
    case FLOAT:
    case DOUBLE:
    case PRECISE:
    case STRING:
    case BINARY:
    case TIMESTAMP:
    case TIMESPAN:
    case WINDOW:
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
        || this.equals(Type.getPrimitive(TypeName.BINARY))
        || this.equals(Type.getPrimitive(TypeName.TYPECLASS_COMPARABLE));
  }

  /** @return an Avro schema describing this type. */
  public Schema getAvroSchema() {
    return getAvroSchema(mTypeName);
  }

  /**
   * The types available in FlumeBase form a lattice; the meet() function
   * returns a type or type class which can hold all values of both t1 and t2.
   *
   * <p>If no type can form an umbrella over both t1 and t2, this returns
   * null.</p>
   *
   * <p>meet() strives to return the least-upper-bound of t1 and t2, but
   * programming errors may return a more permissive upper bound. These
   * should be improved as they are identified.</p>
   *
   * <p>The general rules are as follows:</p>
   * <ul>
   *   <li>meet(X, X) = X (reflexivity)</li>
   *   <li>meet(X, Y) = meet(Y, X) (commutativity)</li>
   *   <li>meet(X, Y) = Y &amp;&amp; meet(Y, Z) =&gt; meet(X, Z) = Z (transitivity)</li>
   *   <li>Nullativity "factors out": meet(X, NULLABLE Y) = meet(NULLABLE X, NULLABLE Y)
   *     = NULLABLE meet(X, Y)</li>
   *   <li>meet(ANY, X) = X for any X</li>
   *   <li>meet(X, STRING) = STRING for any scalar X</li>
   *   <li>meet(LIST&lt;X&gt;, LIST&lt;Y&gt;) = LIST&lt;meet(X, Y)&gt;</li>
   *   <li>The following lattice defines the scalar types:<pre><tt>
   *                     TYPECLASS_ANY
   *                           |
   *       -----------TYPECLASS_COMPARABLE----------
   *      /          /          \                   \
   *     |      STRING     TYPECLASS_NUMERIC         |  (STRING)
   *     |     /  |       /        |        \        | /
   *  TIMESTAMP   |\      |        |        |     BOOLEAN
   *    /         | ---PRECISE(n)  |        |
   *   |          |\     /         |        |
   *   |          | --PRECISE(53)  |        |
   *   |          |\    |          |        |
   *   |          | --DOUBLE   PRECISE(24)  |
   *   |          |\    |       /           |
   *   |          | --FLOAT-----            |
   *   |          |\    |           PRECISE(0)
   *   |          | ---BIGINT----------/         (STRING)
   *   |           \    |                        /
   *   |   (STRING) ---INT   (BOOLEAN)      TIMESPAN    ----------(TYPECLASS_ANY)-----
   *    \       \       |    /               /         /    /        |        \       \
   *     --------------ANY-------------------     STREAM  FLOW  UNIVERSAL SCALARFUNC WINDOW
   *         ("ANY" must be a column type)         ("non-column" types exist in type
   *                                                space but cannot unify with any column
   *                                                types; they are also not concrete types,
   *                                                with the exception of "WINDOW".)
   *
   *</tt></pre></li>
   *   <li>The lattice ANY -&gt; BINARY -&gt; STRING -&gt; ...  also holds.
   *     Bytes are BASE64-encoded when converted to strings.</li>
   *   <li>An additional rule governs meets over PRECISE(k) types:
   *     meet(PRECISE(n), PRECISE(m)) = PRECISE(MAX(n, m))</li>
   *   <li>The following lattice describes TYPECLASS_COMPARABLE and TYPECLASS_ANY:<pre><tt>
   *                 TYPECLASS_ANY
   *                        |
   *             TYPECLASS_COMPARABLE
   *            /           |        \
   * TYPECLASS_NUMERIC   STRING   BOOLEAN
   *</tt></pre></li>
   *   <li>meet(X, UniversalType(C1, C2..Cn)) = X iff meet(X, Ci) = Ci for all constraints,
   *     and TYPECLASS_ANY otherwise.</li>
   *
   *   <li>(TODO: unimplemented)
   *       meet(FLOW(t1, ..., tn), FLOW(t'1, ..., t'n) = meet(FLOW(t''i) for t''i = MEET(ti,t'i)).
   *   </li>
   * </ul>
   * </ul>
   *
   */
  public static Type meet(final Type t1, final Type t2) {
    LOG.debug("Checking meet: " + t1 + " and " + t2);
    assert t1 != null;
    assert t2 != null;

    // Reflexivity
    if (t1.equals(t2)) {
      return t1;
    }

    // Nullable factorization
    if (t1.isNullable() || t2.isNullable()) {
      // factor out nullability.
      Type t1n = t1;
      boolean genuine = false;
      if (t1 instanceof NullableType) {
        t1n = ((NullableType) t1).getInnerType();
        genuine = true;
      }

      Type t2n = t2;
      if (t2 instanceof NullableType) {
        t2n = ((NullableType) t2).getInnerType();
        genuine = true;
      }

      if (genuine) {
        // In the case where one side is a Universal type that meets the properties
        // for nullability, we won't be able to factor that out here. Buf if
        // one of the two sides was in a proper NullableType wrapper, do this
        // recursive step.
        Type nullableMeet = meet(t1n, t2n);
        if (nullableMeet instanceof UniversalType) {
          return nullableMeet; // UniversalType holds nullability inside.
        } else {
          // For everything else, "properly" factor out nullability.
          return new NullableType(nullableMeet);
        }
      }
    }

    // If one type is a Universal Type, then obey the universal type lattice.
    if (t1 instanceof UniversalType || t2 instanceof UniversalType) {
      return meetUniversal(t1, t2);
    }

    // If one type is TYPECLASS_ANY, then it wins. Always.
    if (t1.equals(Type.getPrimitive(Type.TypeName.TYPECLASS_ANY))) {
      return t1;
    } else if (t2.equals(Type.getPrimitive(Type.TypeName.TYPECLASS_ANY))) {
      return t2;
    }

    // Otherwise, if one type is TYPECLASS_COMPARABLE, then it takes everything too.
    if (t1.equals(Type.getPrimitive(Type.TypeName.TYPECLASS_COMPARABLE))) {
      return t1;
    } else if (t2.equals(Type.getPrimitive(Type.TypeName.TYPECLASS_COMPARABLE))) {
      return t2;
    }

    // If one type is ANY, then use the other type. ANY promotes to everything.
    if (t1.equals(Type.getPrimitive(Type.TypeName.ANY))) {
      return t2;
    } else if (t2.equals(Type.getPrimitive(Type.TypeName.ANY))) {
      return t1;
    }

    if (t1 instanceof ListType && t2 instanceof ListType) {
      // Factor out meet under lists.
      ListType lst1 = (ListType) t1;
      ListType lst2 = (ListType) t2;
      Type listMeet = meet(lst1.getElementType(), lst2.getElementType());
      return new ListType(listMeet);
    }

    if (t1.isNumeric() && t2.isNumeric()) {
      return meetNumeric(t1, t2);
    }

    // If one type is STRING, then any scalar type will promote to it.
    if (t1.equals(Type.getPrimitive(Type.TypeName.STRING)) && t2.isScalar()) {
      return t1;
    } else if (t2.equals(Type.getPrimitive(Type.TypeName.STRING)) && t1.isScalar()) {
      return t2;
    }

    // If we're down here, then we either had a weird type (FLOW, STREAM, etc.
    // that doesn't fit with anything, or we had incomparable scalar types:
    // BOOLEAN and TIMESTAMP, etc.
    // Technically, all scalar types should all meet under 'STRING', but that
    // would ruin any semblence of type-safety; it would toString() everything
    // before use.
    // I'm going to hold off before allowing that. We may later investigate
    // the notion of a 'lazy' mode that allows those sorts of expressions, but
    // not right now.

    return Type.getPrimitive(TypeName.TYPECLASS_ANY);
  }

  /**
   * @return the meet of two types where one is a universal type.
   */
  private static Type meetUniversal(final Type t1, final Type t2) {
    // If one type is a UniversalType, and the other type is X, then:
    // assert that for each specified constraint, meet(X, specified) == specified.
    // If the assertion holds, return the universal type. Otherwise, return TYPECLASS_ANY.
    UniversalType universalType;
    Type concreteType;

    if (t1 instanceof UniversalType) {
      universalType = (UniversalType) t1;
      concreteType = t2;
      
      assert concreteType.isConcrete();
    } else {
      universalType = (UniversalType) t2;
      concreteType = t1;
    }

    LOG.debug("meeting concrete " + concreteType + " vs universal " + universalType);

    List<Type> constraints = universalType.getConstraints();
    for (Type constraint : constraints) {
      if (!meet(concreteType, constraint).equals(constraint)) {
        LOG.debug("(no fit; returning TYPECLASS_ANY)");
        return Type.getPrimitive(TypeName.TYPECLASS_ANY);
      }
    }
    
    LOG.debug("ok!");
    return universalType;
  }

  /**
   * @return the meet of t1 and t2, assuming these are both under
   * TYPECLASS_NUMERIC.
   */
  private static Type meetNumeric(final Type t1, final Type t2) {
    if (t1.getPrimitiveTypeName().equals(TypeName.PRECISE)
        && t2.getPrimitiveTypeName().equals(TypeName.PRECISE)) {
      return meetPrecise(t1, t2);
    }

    // Sort the types;
    int compare = t1.getPrimitiveTypeName().meetCompareLevel(t2.getPrimitiveTypeName());
    Type lesserType;
    Type greaterType;
    if (compare == 0) {
      return t1; // They're the same
    } else if (compare > 0) {
      lesserType = t2;
      greaterType = t1;
    } else {
      lesserType = t1;
      greaterType = t2;
    }

    if (greaterType.getPrimitiveTypeName().equals(TypeName.PRECISE)) {
      // The other one is not precise, or it would have been meetPrecise'd above.
      // Convert the lesser type into the lowest precise type that holds it,
      // and meet that with the greaterType.
      PreciseType lesserPrecise;

      switch (lesserType.getPrimitiveTypeName()) {
      case INT:
      case BIGINT:
        lesserPrecise = new PreciseType(0);
        break;
      case FLOAT:
        lesserPrecise = new PreciseType(24); // JLS 4.2.3: 'float' holds 10^-24 precision
        break;
      case DOUBLE:
        lesserPrecise = new PreciseType(53); // JLS 4.2.3: 'double' holds 10^-53 precision
        break;
      default:
        assert lesserType.isNumeric();
        throw new RuntimeException("meetNumeric called on non-numeric " + lesserType);
      }

      return meetPrecise(lesserPrecise, greaterType);
    }

    // For meets of the unparameterized numeric types, go with the greater ranked one.
    return greaterType;
  }

  /**
   * @return meet(PRECISE(n), PRECISE(m)) == PRECISE(MAX(n, m))
   */
  private static Type meetPrecise(final Type t1, final Type t2) {
    final PreciseType p1 = PreciseType.toPreciseType(t1);
    final PreciseType p2 = PreciseType.toPreciseType(t2);

    if (p1.compareTo(p2) < 0) {
      return p2;
    } else {
      return p1;
    }
  }

  /**
   * @return true if a value of this type can be represented in the form of 'other'.
   * For example, INT promotesTo NULLABLE(INT).
   *
   * <p>X promotesTo Y iff meet(X, Y) == Y</p>
   */
  public boolean promotesTo(Type other) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking: " + this + " promotesTo " + other);
    }

    return meet(this, other).equals(other);
  }

  /** @return an Avro schema describing the specified TypeName. */
  protected Schema getAvroSchema(TypeName typeName) { 
    switch (typeName) {
    case ANY:
      return Schema.create(Schema.Type.NULL);
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
    case BINARY:
      return Schema.create(Schema.Type.BYTES);
    case TIMESPAN:
      return TimeSpan.SCHEMA$;
    case TIMESTAMP:
      return Timestamp.SCHEMA$;
    default:
      LOG.error("Cannot create avro schema for type: " + toString());
      return null;
    }
  }

  /**
   * @return this type structure with any UniversalType instances
   * replaced by their concrete types.
   * 
   * @param universalMapping a map from universal type instances to their
   * concrete replacements within an expression.
   * @throws TypeCheckException if the universal type is not bound properly.
   */
  public Type replaceUniversal(Map<Type, Type> universalMapping) throws TypeCheckException {
    // Ordinary scalar type, etc. is not universal to begin with.
    return this;
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

  /**
   * Given a value of another type, return the value,
   * coerced into our own type. 
   * @param valType the type of the original value that needs to be
   * converted. A precondition of this method is that valType.promotesTo(this).
   * @param val the raw value that needs to be converted.
   */
  public Object coerceValue(Type valType, Object val) {
    assert valType != null;
    assert valType.promotesTo(this) || valType.promotesTo(this.asNullable());

    assert isPrimitive();

    if (null == val) {
      return null; // Not much we can do with this.
    } else if (valType.equals(this)) {
      return val;
    } else if (getPrimitiveTypeName().equals(Type.TypeName.STRING)) {
      // coerce this object to a string.
      if (val instanceof ByteBuffer) {
        return BIN2STR_FN.eval(null, val);
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append(val);
        return new Utf8(sb.toString());
      }
    } else if (getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      return Integer.valueOf(((Number) val).intValue());
    } else if (getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      return Long.valueOf(((Number) val).longValue());
    } else if (getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      return Float.valueOf(((Number) val).floatValue());
    } else if (getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      return Double.valueOf(((Number) val).doubleValue());
    }

    throw new RuntimeException("Do not know how to coerce from " + valType
        + " to " + this);
  }
}
