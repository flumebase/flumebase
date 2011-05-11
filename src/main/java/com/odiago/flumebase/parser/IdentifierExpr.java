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

import java.nio.ByteBuffer;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Timestamp;
import com.odiago.flumebase.lang.Type;

/**
 * Expression returning the value of a named field or alias.
 */
public class IdentifierExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(
      IdentifierExpr.class.getName());

  /** 'magic' identifier for the 'host' field of the event. */
  public static final String HOST_IDENTIFIER = "#host";

  /** magic identifier for the 'priority' field of the event. */
  public static final String PRIORITY_IDENTIFIER = "#priority";

  /** magic identifier for the 'timestamp' field of the event. */
  public static final String TIMESTAMP_IDENTIFIER = "#timestamp";
  
  /**
   * Enumeration that identifies the types of accesses an identifier
   * can make into an event.
   */
  public static enum AccessType {
    FIELD, // Ordinary named field
    ATTRIBUTE, // Named #attribute
    HOST,
    PRIORITY,
    TIMESTAMP,
  }

  /** The field this represents. */
  private String mIdentifier;

  /** The type of access to be performed during eval().
   *  May use mAssignedName.
   */
  private AccessType mAccessType;

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

  public void setAccessType(AccessType accessType) {
    mAccessType = accessType;
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
    if (null != mType) {
      LOG.debug("Returning cached type: " + mType);
      return mType;
    }

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
  public Type getResolvedType() {
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
    if (null == sym) {
      // Magic field or attribute.
      return Collections.singletonList(new TypedField("_" + mAssignedName, mType));
    } else {
      sym = sym.resolveAliases();
      String canonicalName = sym.getName();
      TypedField field = new TypedField(canonicalName, sym.getType(), mAssignedName, canonicalName);
      return Collections.singletonList(field);
    }
  }

  @Override
  public Object eval(EventWrapper e) throws IOException {
    switch (mAccessType) {
    case FIELD:
      return e.getField(new TypedField(mAssignedName, mType));
    case ATTRIBUTE:
      byte[] bytes = e.getEvent().getAttrs().get(mAssignedName);
      if (null == bytes) {
        return null;
      } else {
        return ByteBuffer.wrap(bytes);
      }
    case HOST:
      return e.getEvent().getHost();
    case PRIORITY:
      return e.getEvent().getPriority().toString();
    case TIMESTAMP:
      return new Timestamp(e.getEvent().getTimestamp(), e.getEvent().getNanos());
    default:
      throw new IOException("IdentifierExpr.eval() cannot understand mAccessType="
         + mAccessType);
    }
  }

  @Override
  public boolean isConstant() {
    return false;
  }

  @Override
  public boolean requiresEval() {
    return !mAccessType.equals(AccessType.FIELD);
  }
}
