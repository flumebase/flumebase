// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.List;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * An expression which evaluates to a value inside a record.
 */
public abstract class Expr extends SQLStatement {


  /** @return a compact string representation of this expression without line breaks. */
  public abstract String toStringOneLine();

  /**
   * @return the type of this expression with a given set of symbols,
   * or null if no type can be reconciled.
   */
  public abstract Type getType(SymbolTable symTab);

  /**
   * @return the list of all TypedFields required to evaluate the expression.
   */
  public abstract List<TypedField> getRequiredFields(SymbolTable symTab);

  /**
   * Evaluate this expression, pulling identifiers from the input event wrapper.
   */
  public abstract Object eval(EventWrapper inWrapper) throws IOException;

  /**
   * @return the type of this node after type checking is complete.
   * The typechecker will set the type inside the node so it does not
   * need to rely on a symobl table at run time.
   */
  abstract Type getResolvedType();


  /**
   * @return true if this expression is constant.
   */
  public abstract boolean isConstant();

  /**
   * @return an object representing the same value as 'val' but coerced
   * from valType into targetType.
   */
  protected Object coerce(Object val, Type valType, Type targetType) {
    if (null == val) {
      return null;
    } else if (valType.equals(targetType)) {
      return val;
    } else if (targetType.getPrimitiveTypeName().equals(Type.TypeName.STRING)) {
      // coerce this object to a string.
      StringBuilder sb = new StringBuilder();
      sb.append(val);
      return sb.toString();
    } else if (targetType.getPrimitiveTypeName().equals(Type.TypeName.INT)) {
      return Integer.valueOf(((Number) val).intValue());
    } else if (targetType.getPrimitiveTypeName().equals(Type.TypeName.BIGINT)) {
      return Long.valueOf(((Number) val).longValue());
    } else if (targetType.getPrimitiveTypeName().equals(Type.TypeName.FLOAT)) {
      return Float.valueOf(((Number) val).floatValue());
    } else if (targetType.getPrimitiveTypeName().equals(Type.TypeName.DOUBLE)) {
      return Double.valueOf(((Number) val).doubleValue());
    } else {
      throw new RuntimeException("Do not know how to coerce from " + valType
          + " to " + targetType);
    }
  }
}
