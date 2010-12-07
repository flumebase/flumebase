// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.lang.Type;

/**
 * A field in a stream definition; contains the field name,
 * type, any default values or other attributes, etc.
 */
public class TypedField {
  /**
   * Final name of the field after all projection is over.  This will be
   * assigned as the AvroName after passing through the projection layer.
   */
  private String mProjectedName;

  private Type mType;

  /**
   * Name of the field to request in a serialized avro record, or from any
   * Event body.  This may match a user-provided name, but it may also be
   * generated in the case of expressions, etc.
   */
  private String mAvroName;

  /**
   * Separate name to display as console output, etc; this is not necessarily
   * in a canonical form -- it may be '1 + 4', or another description of the
   * expression.
   */
  private String mDisplayName;

  public TypedField(String name, Type type) {
    this(name, type, name, name);
  }

  public TypedField(String name, Type type, String avroName, String displayName) {
    mProjectedName = name;
    mType = type;
    mAvroName = avroName;
    mDisplayName = displayName;
  }

  public String getProjectedName() {
    return mProjectedName;
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
    return mAvroName + "/" + mProjectedName + "/(" + mDisplayName + ") " + mType;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }
    TypedField other = (TypedField) otherObj;
    return other.mProjectedName.equals(mProjectedName) && other.mType.equals(mType)
        && other.mAvroName.equals(mAvroName) && other.mDisplayName.equals(mDisplayName);
  }
}
