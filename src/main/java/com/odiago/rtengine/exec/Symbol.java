// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.lang.Type;

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
