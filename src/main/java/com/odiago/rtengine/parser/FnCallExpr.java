// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FnSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.EvalException;
import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeCheckException;

import com.odiago.rtengine.util.StringUtils;

/**
 * Function call expression.
 * fn(e1, e2, e3...)
 */
public class FnCallExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(FnCallExpr.class.getName());

  private String mFunctionName;
  private List<Expr> mArgExprs; // List of expressions to evaluate for arguments.
  private List<Type> mExprTypes; // List of types returned by the arg expressions.
  private List<Type> mArgTypes; // The types of the arguments themselves, to coerce results to.

  /** The symbol as resolved from a symbol table defining the executable function. */
  private FnSymbol mFnSymbol;

  public FnCallExpr(String fnName) {
    mFunctionName = fnName;
    mArgExprs = new ArrayList<Expr>();
    mExprTypes = new ArrayList<Type>();
    mArgTypes = new ArrayList<Type>();
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
    boolean first = true;
    for (Expr arg : mArgExprs) {
      if (!first) {
        sb.append(", ");
      }

      sb.append(arg.toStringOneLine());
      first = false;
    }

    sb.append(")");
    return sb.toString();
  }

  /** Look up the return type of the function and return it. */
  @Override
  public Type getType(SymbolTable symTab) {
    Symbol fnSymbol = symTab.resolve(mFunctionName);
    if (null == fnSymbol) {
      return null;
    }

    if (!(fnSymbol instanceof FnSymbol)) {
      // This symbol isn't a function call?
      return null;
    }

    mFnSymbol = (FnSymbol) fnSymbol; // memoize this for later.
    return getResolvedType();
  }
  
  /**
   * Called by the type checker to compare the types of the function's arguments
   * against the types returned by the argument expressions.
   * @throws TypeCheckException if an expression type cannot be promoted to
   * an argument type.
   */
  public void resolveArgTypes(SymbolTable symTab) throws TypeCheckException {
    getType(symTab); // Make sure mFnSymbol is resolved.
    if (null == mFnSymbol) {
      throw new TypeCheckException("No such function: " + mFunctionName);
    }
    mArgTypes = mFnSymbol.getArgumentTypes();

    if (mArgExprs.size() != mArgTypes.size()) {
      // Check that arity matches.
      throw new TypeCheckException("Function " + mFunctionName + " requires "
          + mArgTypes.size() + " arguments, but received " + mArgExprs.size());
    }

    for (int i = 0; i < mArgExprs.size(); i++) {
      Type exprType = mArgExprs.get(i).getType(symTab);
      mExprTypes.add(exprType);
      if (!exprType.promotesTo(mArgTypes.get(i))) {
        throw new TypeCheckException("Invalid argument to function " + mFunctionName
            + ": argument " + i + " has type " + exprType + "; requires type "
            + mArgTypes.get(i));
      }
    }
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
  public Object eval(EventWrapper e) throws IOException {
    // TODO(aaron): Reuse arrays between calls.
    Object[] results = new Object[mArgExprs.size()];
    assert(mArgExprs.size() == mArgTypes.size());
    assert(mExprTypes.size() == mArgTypes.size());

    // Evaluate arguments left-to-right.
    for (int i = 0; i < mArgExprs.size(); i++) {
      Object result = mArgExprs.get(i).eval(e);
      results[i] = coerce(result, mExprTypes.get(i), mArgTypes.get(i));
    }

    try {
      return mFnSymbol.getFuncInstance().eval(results);
    } catch (EvalException ee) {
      throw new IOException(ee);
    }
  }

  @Override
  public Type getResolvedType() {
    return mFnSymbol.getReturnType();
  }

}
