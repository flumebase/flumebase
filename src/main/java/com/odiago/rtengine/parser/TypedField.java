// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.lang.Type;

/**
 * A field in a stream definition; contains the field name,
 * type, any default values or other attributes, etc.
 */
public class TypedField {
  /** User-accessible name of the field (reference for other expressions, etc). */
  private String mName;

  private Type mType;

  /** Name of the field to request in a serialized avro record.
   * This may match a user-provided name, but it may also be generated
   * in the case of expressions, etc.
   */
  private String mAvroName;

  /** Separate name to display as console output, etc; this is not
   * necessarily in a canonical form -- it may be '1 + 4', or another
   * description of the expression.
   */
  private String mDisplayName;

  public TypedField(String name, Type type) {
    this(name, type, name, name);
  }

  public TypedField(String name, Type type, String avroName, String displayName) {
    mName = name;
    mType = type;
    mAvroName = avroName;
    mDisplayName = displayName;
  }

  public String getName() {
    return mName;
  }

  public Type getType() {
    return mType;
  }

  public String getAvroName() {
    return mAvroName;
  }

  public String getDisplayName() {
    return mDisplayName;
  }

  @Override
  public String toString() {
    return mName + " " + mType;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }
    TypedField other = (TypedField) otherObj;
    return other.mName.equals(mName) && other.mType.equals(mType)
        && other.mAvroName.equals(mAvroName) && other.mDisplayName.equals(mDisplayName);
  }
}
