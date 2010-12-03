// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Type lhsType = mLeftExpr.getType(symTab);
    Type rhsType = mRightExpr.getType(symTab);
    Type sharedType = null;
    if (lhsType.promotesTo(rhsType)) {
      sharedType = rhsType;
    } else if (rhsType.promotesTo(lhsType)) {
      sharedType = lhsType;
    }

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
}
