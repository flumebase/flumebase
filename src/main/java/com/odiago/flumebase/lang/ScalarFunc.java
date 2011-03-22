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

/**
 * A scalar function that takes in a tuple of fixed arity and returns
 * a single value.
 *
 * <p>Instances of this class should be stateless; repeated calls to eval() to
 * apply the function to different sets of arguments should work without
 * regard to the order in which the calls are made.</p>
 */
public abstract class ScalarFunc extends Function {
  /**
   * Apply the function to its arguments and return its result.
   * @throws EvalException if the function cannot be evaluated (for example,
   * there are not enough arguments, etc.).
   */
  public abstract Object eval(Object... args) throws EvalException;
}
