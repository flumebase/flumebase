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
    // The purpose of this function is to recurse into the specified and actual
    // types in tandem, matching the 'a to the best subtree possible in the
    // definition of the actual type.
    //
    // It is a precondition that actualType promotesTo specifiedType.
    //
    // Mappings like 'a  -> INT are trivial.
    // But if we have trees like:
    // specified:                      actual
    //   ListType                      ListType
    //     NullableType                  NullableType
    //       UniversalType 'a              INT
    //
    // .. we need to recursively enter the two types and still match 'a to INT.
    //
    // If the actual type is not deep enough, we still want to identify
    // the UniversalType instance in the specified type, if one exists. In which
    // case, we associate the UniversalType with the constraint 'NULLABLE(ANY)'
    // (effectively leaving it unconstrained).
    // This can happen in the following case:
    // specified:                      actual:
    //   NullableType                    Nullable
    //     ListType                        NULL
    //       Nullable
    //         Universal 'a
    //
    // This is a valid promotesTo case. NULL is the only type that can
    // promote to a recursive type instance.
    LOG.debug("Checking constraints for specified type: " + specifiedType);
    LOG.debug("  vs actual type: " + actualType);

    assert actualType.promotesTo(specifiedType);

    if (specifiedType instanceof UniversalType) {
      // We've found a match
      mUniversalType = (UniversalType) specifiedType;
      mConstraintType = actualType;
      LOG.debug("Binding constraint: " + mUniversalType + " --> " + mConstraintType);
      return true;
    } else if (specifiedType instanceof ListType) {
      if (actualType instanceof ListType) {
        // Check if List<'a> matches List<actual_constraint>
        return extractConstraint(((ListType) specifiedType).getElementType(),
            ((ListType) actualType).getElementType());
      } else {
        assert Type.TypeName.ANY.equals(actualType.getPrimitiveTypeName());
        return extractConstraint(((ListType) specifiedType).getElementType(),
            Type.getNullable(Type.TypeName.ANY));
      }
    } else if (specifiedType instanceof NullableType) {
      if (actualType instanceof NullableType) {
        // Recurse inside nullable.
        return extractConstraint(((NullableType) specifiedType).getInnerType(),
            ((NullableType) actualType).getInnerType());
      } else {
        // Actual instance os not null, so it's not cast as one. Descend on specifiedType only.
        return extractConstraint(((NullableType) specifiedType).getInnerType(),
            actualType);
      }
    }

    // No UniversalType instance to be found.
    return false;
  }
}
