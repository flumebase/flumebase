// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.Collections;
import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

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

  @Override
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

  @Override
  public String toStringOneLine() {
    // TODO: This needs to escape/enclose literal strings in 'single quotes with \'escapes\''.
    if (null == mValue) {
      return "NULL";
    } else {
      return mValue.toString();
    }
  }

  @Override
  public Type getType(SymbolTable symTab) {
    return getType();
  }
    
  @Override
  public List<TypedField> getRequiredFields(SymbolTable symtab) {
    return Collections.emptyList();
  }

  @Override
  public Object eval(EventWrapper e) {
    // return the value of this const expression; ignore our input data.
    return getValue();
  }

  @Override
  public Type getResolvedType() {
    return mType;
  }

  @Override
  public boolean isConstant() {
    return true;
  }
}
