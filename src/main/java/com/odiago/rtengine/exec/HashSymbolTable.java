// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.HashMap;
import java.util.Iterator;

/**
 * HashMap-backed (transient) symbol table.
 */
public class HashSymbolTable extends SymbolTable {
  /** The actual lookup table for symbols in our scope. */
  private HashMap<String, Symbol> mTable;

  /** Reference to the parent symbol table, if any. */
  private SymbolTable mParent;

  public HashSymbolTable() {
    this(null);
  }

  public HashSymbolTable(SymbolTable parent) {
    mParent = parent;
    mTable = new HashMap<String, Symbol>();
  }

  @Override
  public SymbolTable getParent() {
    return mParent;
  }

  @Override
  public Symbol resolve(String symName) {
    Symbol sym = mTable.get(symName);
    if (null == sym && null != mParent) {
      return mParent.resolve(symName);
    } else {
      return sym;
    }
  }

  @Override
  public void addSymbol(Symbol sym) {
    mTable.put(sym.getName(), sym);
  }

  @Override
  public void remove(String name) {
    mTable.remove(name);
  }

  @Override
  public Iterator<Symbol> iterator() {
    return new LinkedIterator(mTable.values().iterator());
  }

  @Override
  public Iterator<Symbol> levelIterator() {
    return mTable.values().iterator();
  }
}
