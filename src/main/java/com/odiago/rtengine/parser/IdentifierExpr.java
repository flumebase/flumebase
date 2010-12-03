// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * Expression returning the value of a named field or alias.
 */
public class IdentifierExpr extends Expr {

  private String mIdentifier;

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

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    Symbol sym = symTab.resolve(mIdentifier);
    TypedField field = new TypedField(sym.getName(), sym.getType());
    return Collections.singletonList(field);
  }
}
