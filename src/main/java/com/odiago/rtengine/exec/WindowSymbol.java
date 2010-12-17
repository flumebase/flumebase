// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.parser.WindowSpec;

/**
 * A named Window in a SymbolTable.
 */
public class WindowSymbol extends Symbol {
  private WindowSpec mWindowSpec;

  public WindowSymbol(String name, WindowSpec spec) {
    super(name, Type.getPrimitive(Type.TypeName.WINDOW));
    mWindowSpec = spec;
  }


  @Override
  public boolean equals(Object other) {
    boolean parentEquals = super.equals(other);
    if (!parentEquals) {
      return false;
    }

    WindowSymbol sym = (WindowSymbol) other;
    return mWindowSpec.equals(sym.mWindowSpec);
  }

  public WindowSpec getWindowSpec() {
    return mWindowSpec;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ mWindowSpec.hashCode();
  }

  @Override
  public Symbol withName(String name) {
    return new WindowSymbol(name, mWindowSpec);
  }
}
