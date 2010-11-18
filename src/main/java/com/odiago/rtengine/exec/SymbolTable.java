// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

/**
 * Holds all the symbol definitions available to the user in the current
 * session. e.g., named streams, flows, etc. Also contains the metadata
 * associated with each one. This may be stored locally in memory (for a
 * transient symbol table) or backed by a database metastore, etc.
 *
 * SymbolTables may shadow other symbol tables to provide localized
 * contexts. For example, within a SELECT statement, the symbol table
 * contains the column aliases present in the statement.  
 */
public abstract class SymbolTable {

  /**
   * @return the parent symbol table, or null if this is the root.
   */
  protected abstract SymbolTable getParent();

  /**
   * @return the Symbol object associated with symName, or null if that
   * symbol cannot be found.
   */
  public abstract Symbol resolve(String symName);

  public abstract void addSymbol(Symbol sym);
}
