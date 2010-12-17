// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.Iterator;
import java.util.NoSuchElementException;

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
public abstract class SymbolTable implements Iterable<Symbol> {

  /**
   * @return the parent symbol table, or null if this is the root.
   */
  public abstract SymbolTable getParent();

  /**
   * @return the Symbol object associated with symName, or null if that
   * symbol cannot be found.
   */
  public abstract Symbol resolve(String symName);

  /**
   * @return the Symbol object associated with symName in the current
   * table's scope only (do not recursively search ancestor symbol tables).
   */
  public abstract Symbol resolveLocal(String symName);

  /**
   * Add the specified Symbol to our table.
   */
  public abstract void addSymbol(Symbol sym);

  /**
   * Remove the first symbol we find with the specified name.
   */
  public abstract void remove(String symName);

  /**
   * @return An iterator over all symbols available in this scope.
   */
  public abstract Iterator<Symbol> iterator(); 

  /**
   * @return An iterator over all symbols in the current table level only.
   * (a non-recursive iterator.)
   */
  public abstract Iterator<Symbol> levelIterator();

  /**
   * An iterator that will wrap around an iterator for the current level,
   * but also continue iteration at higher levels in the symbol table.
   */
  protected class LinkedIterator implements Iterator<Symbol> {
    /**
     * Set to true if we're iterating over the current level,
     * false if we're iterating on the parent.
     */
    private boolean mThisLevel;

    /** The current inner iterator. */
    private Iterator<Symbol> mIter;

    /**
     * Create an iterator over this symbol table.
     * @param thisLevelIter - an iterator over the contents of the current level of
     * the symbol table.
     */
    public LinkedIterator(Iterator<Symbol> thisLevelIter) {
      mIter = thisLevelIter;
      mThisLevel = true;
    }

    @Override
    public boolean hasNext() {
      if (null == mIter) {
        return false;
      }
      boolean thisHasNext = mIter.hasNext();
      if (thisHasNext) {
        // Current level's iterator is still full.
        return true;
      } else if (mThisLevel) {
        // We've exhausted the iterator, and we are on the current level. Switch to the parent.
        parentIterator();
        return hasNext();
      } else {
        // Done with the parent level too.
        mIter = null;
        return false;
      }
    }

    /**
     * If we're done iterating over the current level, start again on the parent's
     * iterator.
     */
    private void parentIterator() {
      if (!mThisLevel) {
        // We're already done with the parent's level.
        mIter = null;
        return;
      }

      mThisLevel = false;
      SymbolTable parent = getParent();
      if (null != parent) {
        mIter = parent.iterator();
      } else {
        mIter = null;
      }
    }

    @Override
    public Symbol next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (null == mIter) {
        throw new NoSuchElementException();
      }
      return mIter.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Print the SymbolTable's contents to the terminal.
   */
  public void printSymbolTable() {
    StringBuilder sb = new StringBuilder();
    for (Symbol s : this) {
      sb.append(s);
      sb.append("\n");
    }

    System.out.println(sb.toString());
  }

  /**
   * Add all symbols from the specified table to our table.
   */
  public void addAll(SymbolTable table) {
    for (Symbol s : table) {
      addSymbol(s);
    }
  }

  /**
   * @return a new SymbolTable containing all symbols at our current level only.
   */
  public SymbolTable cloneLevel() {
    Iterator<Symbol> it = levelIterator();
    HashSymbolTable hst = new HashSymbolTable(null);
    while (it.hasNext()) {
      hst.addSymbol(it.next());
    }

    return hst;
  }
}
