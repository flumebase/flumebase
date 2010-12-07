// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

/**
 * Binary operator expression.
 */
public class BinExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(BinExpr.class.getName());
  private BinOp mOp;
  private Expr mLeftExpr;
  private Expr mRightExpr;
  private Type mType; // resolved by the type checker.
  private Type mLhsType;
  private Type mRhsType;
  private Type mArgType; // type for the inputs to be coerced to.

  public BinExpr(Expr leftExpr, BinOp op, Expr rightExpr) {
    mLeftExpr = leftExpr;
    mOp = op;
    mRightExpr = rightExpr;
  }

  public BinOp getOp() {
    return mOp;
  }

  public Expr getLeftExpr() {
    return mLeftExpr;
  }

  public Expr getRightExpr() {
    return mRightExpr;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("BinExpr mOp=");
    sb.append(mOp);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("left expr:\n");
    mLeftExpr.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("right expr:\n");
    mRightExpr.format(sb, depth + 2);
  }

  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    sb.append("(");
    sb.append(mLeftExpr.toStringOneLine());
    sb.append(") ");
    sb.append(symbolForOp(mOp));
    sb.append(" (");
    sb.append(mRightExpr.toStringOneLine());
    sb.append(")");
    return sb.toString();
  }

  private static String symbolForOp(BinOp op) {
    switch (op) {
    case Times:
      return "*";
    case Div:
      return "/";
    case Mod:
      return "%";
    case Add:
      return "+";
    case Subtract:
      return "-";
    case Greater:
      return ">";
    case GreaterEq:
      return ">=";
    case Less:
      return "<";
    case LessEq:
      return "<=";
    case Eq:
      return "=";
    case NotEq:
      return "!=";
    case IsNot:
      return "IS NOT";
    case Is:
      return "IS";
    case And:
      return "AND";
    case Or:
      return "OR";
    default:
      throw new RuntimeException("symbolForOp does not understand " + op);
    }
  }

  @Override
  public Type getType(SymbolTable symTab) {
    // Get the types of the lhs and rhs, and then verify that one promotes to the other.
    mLhsType = mLeftExpr.getType(symTab);
    mRhsType = mRightExpr.getType(symTab);
    Type sharedType = null;
    if (mLhsType.promotesTo(mRhsType)) {
      sharedType = mRhsType;
    } else if (mRhsType.promotesTo(mLhsType)) {
      sharedType = mLhsType;
    }

    // Save this for later.
    mArgType = sharedType;

    switch (mOp) {
    case Times:
    case Div:
    case Mod:
    case Add:
    case Subtract:
      // Numeric operators return their input type. 
      return sharedType;
    case Greater:
    case GreaterEq:
    case Less:
    case LessEq:
    case Eq:
    case NotEq:
    case IsNot:
    case Is:
    case And:
    case Or:
      return Type.getPrimitive(Type.TypeName.BOOLEAN);
    default:
      // Couldn't reconcile any type.
      LOG.error("Unknown operator " + mOp + " in getType()");
      return null;
    }
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    List<TypedField> out = new ArrayList<TypedField>();
    out.addAll(mLeftExpr.getRequiredFields(symTab));
    out.addAll(mRightExpr.getRequiredFields(symTab));
    return out;
  }

  @Override
  public Object eval(EventWrapper e) throws IOException {
    Object lhs = mLeftExpr.eval(e);
    Object rhs = mRightExpr.eval(e);

    if (null == lhs || null == rhs) {
      // NULL op X always returns null.
      return null;
    }

    if (!mLhsType.equals(mArgType)) {
      lhs = coerce(lhs, mLhsType, mArgType);
    }

    if (!mRhsType.equals(mArgType)) {
      rhs = coerce(rhs, mRhsType, mArgType);
    }

    switch(mOp) {
    case Times:
      switch (mArgType.getPrimitiveTypeName()) {
      case INT:
        return Integer.valueOf(((Number) lhs).intValue() * ((Number) rhs).intValue());
      case BIGINT:
        return Long.valueOf(((Number) lhs).longValue() * ((Number) rhs).longValue());
      case FLOAT:
        return Float.valueOf(((Number) lhs).floatValue() * ((Number) rhs).floatValue());
      case DOUBLE:
        return Double.valueOf(((Number) lhs).doubleValue() * ((Number) rhs).doubleValue());
      default:
        LOG.error("Cannot multiply with non-number type " + mArgType);
        return null;
      }
    case Div:
      switch (mArgType.getPrimitiveTypeName()) {
      case INT:
        return Integer.valueOf(((Number) lhs).intValue() / ((Number) rhs).intValue());
      case BIGINT:
        return Long.valueOf(((Number) lhs).longValue() / ((Number) rhs).longValue());
      case FLOAT:
        return Float.valueOf(((Number) lhs).floatValue() / ((Number) rhs).floatValue());
      case DOUBLE:
        return Double.valueOf(((Number) lhs).doubleValue() / ((Number) rhs).doubleValue());
      default:
        LOG.error("Cannot divide with non-number type " + mArgType);
        return null;
      }
    case Mod:
      switch (mArgType.getPrimitiveTypeName()) {
      case INT:
        return Integer.valueOf(((Number) lhs).intValue() % ((Number) rhs).intValue());
      case BIGINT:
        return Long.valueOf(((Number) lhs).longValue() % ((Number) rhs).longValue());
      case FLOAT:
        return Float.valueOf(((Number) lhs).floatValue() % ((Number) rhs).floatValue());
      case DOUBLE:
        return Double.valueOf(((Number) lhs).doubleValue() % ((Number) rhs).doubleValue());
      default:
        LOG.error("Cannot divide with non-number type " + mArgType);
        return null;
      }
    case Add:
      switch (mArgType.getPrimitiveTypeName()) {
      case INT:
        return Integer.valueOf(((Number) lhs).intValue() + ((Number) rhs).intValue());
      case BIGINT:
        return Long.valueOf(((Number) lhs).longValue() + ((Number) rhs).longValue());
      case FLOAT:
        return Float.valueOf(((Number) lhs).floatValue() + ((Number) rhs).floatValue());
      case DOUBLE:
        return Double.valueOf(((Number) lhs).doubleValue() + ((Number) rhs).doubleValue());
      case STRING:
        // String concatenation.
        StringBuilder sb = new StringBuilder();
        sb.append(lhs);
        sb.append(rhs);
        return sb.toString();
      default:
        LOG.error("Cannot divide with non-number type " + mArgType);
        return null;
      }
    case Subtract:
      switch (mArgType.getPrimitiveTypeName()) {
      case INT:
        return Integer.valueOf(((Number) lhs).intValue() - ((Number) rhs).intValue());
      case BIGINT:
        return Long.valueOf(((Number) lhs).longValue() - ((Number) rhs).longValue());
      case FLOAT:
        return Float.valueOf(((Number) lhs).floatValue() - ((Number) rhs).floatValue());
      case DOUBLE:
        return Double.valueOf(((Number) lhs).doubleValue() - ((Number) rhs).doubleValue());
      default:
        LOG.error("Cannot divide with non-number type " + mArgType);
        return null;
      }
    case Greater:
      return Boolean.valueOf(((Comparable) lhs).compareTo(rhs) > 0);
    case GreaterEq:
      return Boolean.valueOf(((Comparable) lhs).compareTo(rhs) >= 0);
    case Less:
      return Boolean.valueOf(((Comparable) lhs).compareTo(rhs) < 0);
    case LessEq:
      return Boolean.valueOf(((Comparable) lhs).compareTo(rhs) <= 0);
    case Eq:
      return Boolean.valueOf(lhs.equals(rhs));
    case NotEq:
      return Boolean.valueOf(!lhs.equals(rhs));
    case IsNot:
      LOG.error("do not know how to compute IS NOT as Boolexp");
    case Is:
      LOG.error("do not know how to compute IS as Boolexp");
    case And:
      return Boolean.valueOf(((Boolean) lhs).booleanValue()
          && ((Boolean) rhs).booleanValue());
    case Or:
      return Boolean.valueOf(((Boolean) lhs).booleanValue()
          || ((Boolean) rhs).booleanValue());
    default:
      // Couldn't evaluate this operator.
      throw new RuntimeException("Unknown operator " + mOp + " in eval");
    }
  }

  /**
   * @return an object representing the same value as 'val' but coerced
   * from valType into targetType.
   */
  private Object coerce(Object val, Type valType, Type targetType) {
    System.out.println("valtype: " + valType + " pt=" + valType.getPrimitiveTypeName());
    System.out.println("targettype: " + targetType + " pt=" + targetType.getPrimitiveTypeName());
    if (targetType.getPrimitiveTypeName().equals(Type.TypeName.STRING)) {
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

  // Sets the type that the expression returns
  public void setType(Type t) {
    mType = t;
  }

  @Override
  Type getResolvedType() {
    return mType;
  }

  /**
   * Set the type that the subexpressions should be coerced to.
   */
  private Type getArgType() {
    return mArgType;
  }
}
