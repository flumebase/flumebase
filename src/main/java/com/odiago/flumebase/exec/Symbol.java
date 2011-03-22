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

package com.odiago.flumebase.exec;

import com.odiago.flumebase.lang.Type;

/**
 * A named symbol in a SymbolTable.
 */
public class Symbol {
  private final String mName;
  private final Type mType;

  public Symbol(String name, Type type) {
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
    return mName + " (" + mType + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (null == other) {
      return false;
    } else if (!getClass().equals(other.getClass())) {
      return false;
    }

    Symbol sym = (Symbol) other;
    return mName.equals(sym.mName) && mType.equals(sym.mType);
  }

  @Override
  public int hashCode() {
    return mName.hashCode();
  }

  /**
   * @return the canonical symbol for this entity. Most symbols resolve to
   * themselves.
   */
  public Symbol resolveAliases() {
    return this;
  }

  /**
   * @return a new symbol identical to this one except for the name.
   * The new symbol is not an alias to this one. That is handled by
   * creating an AliasSymbol.
   */
  public Symbol withName(String name) {
    return new Symbol(name, mType);
  }
}
