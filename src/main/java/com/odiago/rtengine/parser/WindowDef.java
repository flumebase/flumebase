// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * Defines a binding from an identifier to a WindowSpec within a scope.
 */
public class WindowDef extends SQLStatement {

  private String mName;
  private WindowSpec mWindowSpec;

  public WindowDef(String name, WindowSpec spec) {
    mName = name;
    mWindowSpec = spec;
  }

  public String getName() {
    return mName;
  }

  public WindowSpec getWindowSpec() {
    return mWindowSpec;
  }

  public void setWindowSpec(WindowSpec windowSpec) {
    mWindowSpec = windowSpec;
  }

  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("WindowDef mName=");
    sb.append(mName);
    sb.append(", windowSpec:\n");
    mWindowSpec.format(sb, depth + 1);
  }
}

