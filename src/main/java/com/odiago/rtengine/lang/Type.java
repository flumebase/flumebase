// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

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
    STRING,
    TIMESTAMP,
    TIMESPAN,
    NULLABLE, // nullable instance of a primitive type (int, bigint, etc).
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
  private static final Map<TypeName, Type> NULLABLE_TYPES;

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

    NULLABLE_TYPES = new HashMap<TypeName, Type>();
    NULLABLE_TYPES.put(TypeName.BOOLEAN, new NullableType(TypeName.BOOLEAN));
    NULLABLE_TYPES.put(TypeName.INT, new NullableType(TypeName.INT));
    NULLABLE_TYPES.put(TypeName.BIGINT, new NullableType(TypeName.BIGINT));
    NULLABLE_TYPES.put(TypeName.FLOAT, new NullableType(TypeName.FLOAT));
    NULLABLE_TYPES.put(TypeName.DOUBLE, new NullableType(TypeName.DOUBLE));
    NULLABLE_TYPES.put(TypeName.STRING, new NullableType(TypeName.STRING));
    NULLABLE_TYPES.put(TypeName.TIMESTAMP, new NullableType(TypeName.TIMESTAMP));
    NULLABLE_TYPES.put(TypeName.TIMESPAN, new NullableType(TypeName.TIMESPAN));
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
   * @return true if a null value may be used in this type.
   */
  public boolean isNullable() {
    return false;
  }

  /** @return true if this is a primitive type (Non-recursive) */
  public boolean isPrimitive() {
    return true;
  }

  /** @return an Avro schema describing this type. */
  public Schema getAvroSchema() {
    return getAvroSchema(mTypeName);
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
    case TIMESPAN: // TODO(aaron): Schema for this type.
    case TIMESTAMP: // TODO(aaron): Schema for this type.
    default:
      LOG.error("Cannot create avro schema for type: " + toString());
      return null;
    }
  }

  @Override
  public String toString() {
    return mTypeName.name();
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
