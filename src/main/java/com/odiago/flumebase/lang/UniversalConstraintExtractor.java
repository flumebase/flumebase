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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a pair of types: an argument's specified type, and the expression's
 * actual type, extract any universal type instance from the specified type
 * and the actual constraint in the same 'position' on the actual type.
 */
public class UniversalConstraintExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(
      UniversalConstraintExtractor.class.getName());
  private UniversalType mUniversalType;
  private Type mConstraintType;

  public UniversalType getUniversalType() {
    return mUniversalType;
  }

  public Type getConstraintType() {
    return mConstraintType;
  }

  /**
   * Run the constraint extraction algorithm.
   * @return true if a UniversalType instance was identified,
   * false otherwise.
   */
  public boolean extractConstraint(Type specifiedType, Type actualType) {
    LOG.debug("Checking constraints for specified type: " + specifiedType);
    LOG.debug("  vs actual type: " + actualType);

    if (specifiedType instanceof UniversalType) {
      // We've found a match
      mUniversalType = (UniversalType) specifiedType;
      mConstraintType = actualType;
      LOG.debug("Binding constraint: " + mUniversalType + " --> " + mConstraintType);
      return true;
    } else if (specifiedType instanceof ListType && actualType instanceof ListType) {
      // Check if List<'a> matches List<actual_constraint>
      return extractConstraint(((ListType) specifiedType).getElementType(),
          ((ListType) actualType).getElementType());
    } else if (specifiedType instanceof NullableType && actualType instanceof NullableType) {
      return extractConstraint(((NullableType) specifiedType).getInnerType(),
          ((NullableType) actualType).getInnerType());
    } else if (specifiedType instanceof NullableType) {
      return extractConstraint(((NullableType) specifiedType).getInnerType(), actualType);
    }

    // No UniversalType instance to be found.
    return false;
  }
}
