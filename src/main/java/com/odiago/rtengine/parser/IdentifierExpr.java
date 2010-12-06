// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * Expression returning the value of a named field or alias.
 */
public class IdentifierExpr extends Expr {

  /** The field this represents. */
  private String mIdentifier;

  /** Assigned type after symbol table resolution, in the type checker. */
  private Type mType;

  public IdentifierExpr(String identifier) {
    mIdentifier = identifier;
  }

  public String getIdentifier() {
    return mIdentifier;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("IdentifierExpr mIdentifier=");
    sb.append(mIdentifier);
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

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    Symbol sym = symTab.resolve(mIdentifier);
    TypedField field = new TypedField(sym.getName(), sym.getType());
    return Collections.singletonList(field);
  }

  @Override
  public Object eval(EventWrapper e) throws IOException {
    return e.getField(new TypedField(mIdentifier, mType));
  }
}
