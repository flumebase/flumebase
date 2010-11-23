// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.lang.Type;

/**
 * A field in a stream definition; contains the field name,
 * type, any default values or other attributes, etc.
 */
public class TypedField {
  /** User-accessible name of the field. */
  private String mName;

  private Type mType;

  public TypedField(String name, Type type) {
    mName = name;
    mType = type;
  }

  public String getName() {
    return mName;
  }

  public Type getType() {
    return mType;
  }

  @Override
  public String toString() {
    return mName + " " + mType;
  }
}
