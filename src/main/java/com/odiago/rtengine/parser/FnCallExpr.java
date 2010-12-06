// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.util.StringUtils;

/**
 * Function call expression.
 * fn(e1, e2, e3...)
 */
public class FnCallExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(FnCallExpr.class.getName());
  private String mFunctionName;
  private List<Expr> mArgExprs;

  public FnCallExpr(String fnName) {
    mFunctionName = fnName;
    mArgExprs = new ArrayList<Expr>();
  }

  public String getFunctionName() { 
    return mFunctionName;
  }

  public List<Expr> getArgExpressions() {
    return mArgExprs;
  }

  /**
   * Adds the specified expression to the argument list.
   */
  public void addArg(Expr e) {
    mArgExprs.add(e);
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("FnCallExpr mFunctionName=");
    sb.append(mFunctionName);
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("arguments:\n");
    for (Expr e : mArgExprs) {
      e.format(sb, depth + 2);
    }
  }
  
  @Override
  public String toStringOneLine() {
    StringBuilder sb = new StringBuilder();
    sb.append(mFunctionName);
    sb.append("(");
    StringUtils.formatList(sb, mArgExprs);
    sb.append(")");
    return sb.toString();
  }

  @Override
  public Type getType(SymbolTable symTab) {
    // TODO(aaron): Look up the return type for the function and type check.
    LOG.error("Unimplemented: getType() for function call exec.");
    return null;
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    List<TypedField> out = new ArrayList<TypedField>();
    for (Expr e : mArgExprs) {
      out.addAll(e.getRequiredFields(symTab));
    }

    return out;
  }

  @Override
  public Object eval(EventWrapper e) {
    // TODO(aaron): Implement this.
    throw new RuntimeException("Cannot evaluate function call expr.");
  }

  @Override
  public Type getResolvedType() {
    throw new RuntimeException("Cannot resolve type for fn call expr.");
  }

}
