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

/**
 * Defines a binding from an identifier to a WindowSpec within a scope.
 */
public class WindowDef extends SQLStatement {

  private String mName;
  private WindowSpec mWindowSpec;

  public WindowDef(String name, WindowSpec spec) {
    mName = name;
    mWindowSpec = spec;
  }

  public String getName() {
    return mName;
  }

  public WindowSpec getWindowSpec() {
    return mWindowSpec;
  }

  public void setWindowSpec(WindowSpec windowSpec) {
    mWindowSpec = windowSpec;
  }

  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("WindowDef mName=");
    sb.append(mName);
    sb.append(", windowSpec:\n");
    mWindowSpec.format(sb, depth + 1);
  }
}

