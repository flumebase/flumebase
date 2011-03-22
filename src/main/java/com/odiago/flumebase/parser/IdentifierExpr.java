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

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;

/**
 * Expression returning the value of a named field or alias.
 */
public class IdentifierExpr extends Expr {

  /** The field this represents. */
  private String mIdentifier;

  /**
   * The assigned name of the object to retrieve within the query.
   * This is a unique identifier assigned during type checking.
   */
  private String mAssignedName;

  /** Assigned type after symbol table resolution, in the type checker. */
  private Type mType;

  /**
   * AssignedSymbol instance identified by the TypeChecker as belonging
   * to this IdentifierExpr.
   */
  private AssignedSymbol mAssignedSym;

  public IdentifierExpr(String identifier) {
    mIdentifier = identifier;
  }

  public String getIdentifier() {
    return mIdentifier;
  }

  public String getAssignedName() {
    return mAssignedName;
  }

  public void setAssignedName(String assignedName) {
    mAssignedName = assignedName;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("IdentifierExpr mIdentifier=");
    sb.append(mIdentifier);
    if (mAssignedName != null) {
      sb.append(", mAssignedName=");
      sb.append(mAssignedName);
    }
    sb.append("\n");
  }

  @Override
  public String toStringOneLine() {
    return mIdentifier;
  }

  @Override
  public Type getType(SymbolTable symTab) {
    Symbol sym = symTab.resolve(mIdentifier);
    if (null == sym) {
      return null;
    } else {
      return sym.getType();
    }
  }

  /**
   * Specifies the type of this node to itself, after the type checker
   * has performed all the type resolution.
   */
  public void setType(Type t) {
    mType = t;
  }

  @Override
  Type getResolvedType() {
    return mType;
  }

  /**
   * Sets the AssignedSymbol instance identified by the TypeChecker
   * for this identifier expression.
   */
  public void setAssignedSymbol(AssignedSymbol assignedSym) {
    mAssignedSym = assignedSym;
  }

  public AssignedSymbol getAssignedSymbol() {
    return mAssignedSym;
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    Symbol sym = symTab.resolve(mIdentifier);
    assert null != sym;

    sym = sym.resolveAliases();
    String canonicalName = sym.getName();
    TypedField field = new TypedField(canonicalName, sym.getType(), mAssignedName, canonicalName);
    return Collections.singletonList(field);
  }

  @Override
  public Object eval(EventWrapper e) throws IOException {
    return e.getField(new TypedField(mAssignedName, mType));
  }

  @Override
  public boolean isConstant() {
    return false;
  }
}
