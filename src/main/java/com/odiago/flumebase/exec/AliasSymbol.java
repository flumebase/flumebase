// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

/**
 * A symbol which refers to another existing symbol.  This allows a symbol to
 * be referenced by a different name by the user.  For example, in the context
 * of "SELECT x FROM s", the field may be referenced as 'x' (the primary
 * symbol), or also 's.x' (the alias).
 */
public class AliasSymbol extends Symbol {

  /* The symbol we are aliasing. */
  private Symbol mOriginal;

  public AliasSymbol(String alias, Symbol original) {
    super(alias, original.getType());
    mOriginal = original;
  }

  public Symbol getOriginalSymbol() {
    return mOriginal;
  }

  /**
   * Return the canonical symbol for this entity, removing this alias
   * layer as well as any other nested aliases.
   */
  @Override
  public Symbol resolveAliases() {
    return getOriginalSymbol().resolveAliases();
  }

  @Override
  public boolean equals(Object other) {
    if (!super.equals(other)) {
      return false;
    }

    AliasSymbol sym = (AliasSymbol) other;
    return mOriginal.equals(sym.mOriginal);
  }

  @Override
  public Symbol withName(String name) {
    return new AliasSymbol(name, mOriginal);
  }
}
