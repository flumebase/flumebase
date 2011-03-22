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

import java.util.List;

/**
 * Abstract base class that defines a callable function. Subclasses
 * of this exist for scalar, aggregate, and table functions.
 */
public abstract class Function {
  /**
   * @return the Type of the object returned by the function.
   */
  public abstract Type getReturnType();
  
  /**
   * @return an ordered list containing the types expected for all arguments.
   */
  public abstract List<Type> getArgumentTypes();

  /**
   * Determines whether arguments are promoted to their specified types by
   * the runtime. If this returns true, actual arguments are promoted to
   * new values that match the types specified in getArgumentTypes().
   * If false, the expressions are simply type-checked to ensure that there
   * is a valid promotion, but are passed in as-is. The default value of
   * this method is true.
   */
  public boolean autoPromoteArguments() {
    return true;
  }
}
