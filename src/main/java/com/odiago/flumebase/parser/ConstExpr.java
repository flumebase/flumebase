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

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;

/**
 * An expression returning a constant value with the specified type.
 */
public class ConstExpr extends Expr {
  
  private Type mType;
  private Object mValue;

  public ConstExpr(Type type, Object val) {
    mType = type;
    mValue = val;
  }

  public Type getType() {
    return mType;
  }

  public Object getValue() {
    return mValue;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("ConstExpr\n");
    pad(sb, depth + 1);
    sb.append("mType=");
    sb.append(mType);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("mValue=");
    if (null == mValue) {
      sb.append("null");
    } else {
      sb.append(mValue);
    }
    sb.append("\n");
  }

  @Override
  public String toStringOneLine() {
    // TODO: This needs to escape/enclose literal strings in 'single quotes with \'escapes\''.
    if (null == mValue) {
      return "NULL";
    } else {
      return mValue.toString();
    }
  }

  @Override
  public Type getType(SymbolTable symTab) {
    return getType();
  }
    
  @Override
  public List<TypedField> getRequiredFields(SymbolTable symtab) {
    return Collections.emptyList();
  }

  @Override
  public Object eval(EventWrapper e) {
    // return the value of this const expression; ignore our input data.
    return getValue();
  }

  @Override
  public Type getResolvedType() {
    return mType;
  }

  @Override
  public boolean isConstant() {
    return true;
  }
}
