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

import com.odiago.flumebase.parser.WindowSpec;

/**
 * A named Window in a SymbolTable.
 */
public class WindowSymbol extends AssignedSymbol {
  private WindowSpec mWindowSpec;

  public WindowSymbol(String name, WindowSpec spec) {
    super(name, Type.getPrimitive(Type.TypeName.WINDOW), name);
    mWindowSpec = spec;
  }


  @Override
  public boolean equals(Object other) {
    boolean parentEquals = super.equals(other);
    if (!parentEquals) {
      return false;
    }

    WindowSymbol sym = (WindowSymbol) other;
    return mWindowSpec.equals(sym.mWindowSpec);
  }

  public WindowSpec getWindowSpec() {
    return mWindowSpec;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ mWindowSpec.hashCode();
  }

  @Override
  public Symbol withName(String name) {
    return new WindowSymbol(name, mWindowSpec);
  }
}
