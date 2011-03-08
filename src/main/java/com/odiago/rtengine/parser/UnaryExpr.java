// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * Unary operator expression (negation, logical not).
 */
public class UnaryExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(UnaryExpr.class.getName());
  private UnaryOp mOp;
  private Expr mSubExpr;

  public UnaryExpr(UnaryOp op, Expr subExpr) {
    mOp = op;
    mSubExpr = subExpr;
  }

  public UnaryOp getOp() {
    return mOp;
  }

  public void setOp(UnaryOp op) {
    mOp = op;
  }

  public Expr getSubExpr() {
    return mSubExpr;
  }

  public void setSubExpr(Expr subExpr) {
    mSubExpr = subExpr;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("UnaryExpr mOp=");
    sb.append(mOp);
    sb.append("\n");
    mSubExpr.format(sb, depth + 1);
  }

  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    if (mOp == UnaryOp.IsNull || mOp == UnaryOp.IsNotNull) {
      // These operators are written after their expression.
      sb.append(mSubExpr.toStringOneLine());
      sb.append(" ");
      sb.append(symbolForOp(mOp));
    } else {
      // Unary operator comes first in the normal case.
      sb.append(symbolForOp(mOp));
      sb.append(" ");
      sb.append(mSubExpr.toStringOneLine());
    }
    return sb.toString();
  }

  private static String symbolForOp(UnaryOp op) {
    switch (op) {
    case Plus:
      return "+";
    case Minus:
      return "-";
    case Not:
      return "NOT";
    case IsNotNull:
      return "IS NOT NULL";
    case IsNull:
      return "IS NULL";
    default:
      throw new RuntimeException("symbolForOp does not understand " + op);
    }
  }

  @Override
  public Type getType(SymbolTable symTab) {
    switch (mOp) {
    case Plus:
    case Minus:
      // Numeric operators return their input type.
      return mSubExpr.getType(symTab);
    case Not:
      // logical operator returns boolean.
      return Type.getNullable(Type.TypeName.BOOLEAN);
    case IsNull:
    case IsNotNull:
      // IS (NOT) NULL operators return a non-null boolean.
      return Type.getPrimitive(Type.TypeName.BOOLEAN);
    default:
      throw new RuntimeException("Unary getType() cannot handle operator " + mOp);
    }
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    return this.mSubExpr.getRequiredFields(symTab);
  }

  @Override
  public Type getResolvedType() {
    return mSubExpr.getResolvedType();
  }

  @Override
  public Object eval(EventWrapper e) throws IOException {
    Object childObj = mSubExpr.eval(e);
    Type childType = mSubExpr.getResolvedType();
    switch (mOp) {
    case Plus:
      // Keep the object (number) with the same sign; don't need to do anything.
      return childObj;
    case Minus:
      if (childObj == null) {
        return null;
      }

      switch (childType.getPrimitiveTypeName()) {
      case INT:
        return -((Integer) childObj).intValue();
      case BIGINT:
        return -((Long) childObj).longValue();
      case FLOAT:
        return -((Float) childObj).floatValue();
      case DOUBLE:
        return -((Double) childObj).doubleValue();
      default:
        // Type error; ignore this field.
        LOG.debug("Typechecker failed; got type " + childType);
        return null;
      }
    case Not:
      Boolean b = (Boolean) childObj;
      if (null == b) {
        return null;
      } else if (b.booleanValue()) {
        return Boolean.FALSE;
      } else {
        return Boolean.TRUE;
      }
    case IsNull:
      if (null == childObj) {
        return Boolean.TRUE;
      } else {
        return Boolean.FALSE;
      }
    case IsNotNull:
      if (null == childObj) {
        return Boolean.FALSE;
      } else {
        return Boolean.TRUE;
      }
    default:
      LOG.error("Unknown unary operator : " + mOp);
      return null;
    }
  }

  @Override
  public boolean isConstant() {
    return mSubExpr.isConstant();
  }
}
