// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.lang.Type;

/**
 * An expression returning a constant value with the specified type.
 */
public class ConstExpr extends Expr {
  
  private Type mType;
  private Object mValue;

  public ConstExpr(Type type, Object val) {
    mType = type;
    mValue = val;
  }

  public Type getType() {
    return mType;
  }

  public Object getValue() {
    return mValue;
  }

  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("ConstExpr\n");
    pad(sb, depth + 1);
    sb.append("mType=");
    sb.append(mType);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("mValue=");
    if (null == mValue) {
      sb.append("null");
    } else {
      sb.append(mValue);
    }
    sb.append("\n");
  }
  
}
