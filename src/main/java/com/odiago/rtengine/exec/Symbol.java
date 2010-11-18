// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

/**
 * A named symbol in a SymbolTable.
 */
public class Symbol {
  private final String mName;
  private final SymbolType mType;

  public enum SymbolType {
    STREAM,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    TIMESTAMP,
    TIMERANGE,
  }

  public Symbol(String name, SymbolType type) {
    mName = name;
    mType = type;
  }

  public String getName() {
    return mName;
  }

  public SymbolType getType() {
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
    return mName.equals(sym.mName);
  }

  @Override
  public int hashCode() {
    return mName.hashCode();
  }
}
