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

package com.odiago.flumebase.parser;

import com.odiago.flumebase.lang.Type;

/**
 * A field in a stream definition; contains the field name,
 * type, any default values or other attributes, etc.
 */
public class TypedField {
  /**
   * The user's alias for this field. May be set explicitly "SELECT ... AS foo",
   * or implicitly via "SELECT x", or anonymous if this is for a complex expression.
   */
  private String mUserAlias;

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
    mUserAlias = name;
    mType = type;
    mAvroName = avroName;
    mDisplayName = displayName;
  }

  public String getUserAlias() {
    return mUserAlias;
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
    return mAvroName + "/" + mUserAlias + "/(" + mDisplayName + ") " + mType;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }
    TypedField other = (TypedField) otherObj;
    return other.mUserAlias.equals(mUserAlias) && other.mType.equals(mType)
        && other.mAvroName.equals(mAvroName) && other.mDisplayName.equals(mDisplayName);
  }
}
