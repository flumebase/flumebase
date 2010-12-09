// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FnSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.EvalException;
import com.odiago.rtengine.lang.ScalarFunc;
import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.TypeCheckException;
import com.odiago.rtengine.lang.UniversalType;

/**
 * Function call expression.
 * fn(e1, e2, e3...)
 */
public class FnCallExpr extends Expr {
  private static final Logger LOG = LoggerFactory.getLogger(FnCallExpr.class.getName());

  private String mFunctionName;
  private List<Expr> mArgExprs; // List of expressions to evaluate for arguments.
  private List<Type> mExprTypes; // List of types returned by the arg expressions.

  // The types of the arguments themselves, to coerce results to.
  private Type[] mArgTypes; 

  private Type mReturnType; // The type of the value we return.

  /** The symbol as resolved from a symbol table defining the executable function. */
  private FnSymbol mFnSymbol;
  private ScalarFunc mExecFunc; // The function instance to execute.
  private boolean mAutoPromote; // true if we auto-promote argument return types.
  private Object[] mPartialResults; // reusable array where argument results are stored.

  public FnCallExpr(String fnName) {
    mFunctionName = fnName;
    mArgExprs = new ArrayList<Expr>();
    mExprTypes = new ArrayList<Type>();
    mArgTypes = null;
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
    if (mReturnType == null) {
      try {
        resolveArgTypes(symTab);
      } catch (TypeCheckException tce) {
        LOG.error("TypeCheckException in getType: " + tce);
        return null;
      }
    }

    return getResolvedType();
  }
  
  /**
   * Called by the type checker to compare the types of the function's arguments
   * against the types returned by the argument expressions.
   * Sets mFnSymbol, mExprTypes, mArgTypes, mReturnType.
   * @throws TypeCheckException if an expression type cannot be promoted to
   * an argument type.
   */
  public void resolveArgTypes(SymbolTable symTab) throws TypeCheckException {
    if (null != mReturnType) {
      // Already resolved this expression instance.
      return;
    }

    // Get a handle to the symbol defining the function.
    Symbol fnSymbol = symTab.resolve(mFunctionName);
    if (null == fnSymbol) {
      throw new TypeCheckException("No such function: " + mFunctionName);
    }

    if (!(fnSymbol instanceof FnSymbol)) {
      // This symbol isn't a function call?
      throw new TypeCheckException("Symbol " + mFunctionName + " is not a function");
    }

    mFnSymbol = (FnSymbol) fnSymbol; // memoize this for later.

    // Get a list of argument types from the function symbol. These may include
    // universal types we need to concretize.
    List<Type> abstractArgTypes = mFnSymbol.getArgumentTypes();

    if (mArgExprs.size() != abstractArgTypes.size()) {
      // Check that arity matches.
      throw new TypeCheckException("Function " + mFunctionName + " requires "
          + abstractArgTypes.size() + " arguments, but received " + mArgExprs.size());
    }

    // Check that each expression type can promote to the argument type.
    for (int i = 0; i < mArgExprs.size(); i++) {
      Type exprType = mArgExprs.get(i).getType(symTab);
      mExprTypes.add(exprType);
      if (!exprType.promotesTo(abstractArgTypes.get(i))) {
        throw new TypeCheckException("Invalid argument to function " + mFunctionName
            + ": argument " + i + " has type " + exprType + "; requires type "
            + abstractArgTypes.get(i));
      }
    }

    mArgTypes = new Type[abstractArgTypes.size()];

    // Now identify all the UniversalType instances in here, and the
    // actual constraints on each of these.
    Map<UniversalType, List<Type>> unifications = new HashMap<UniversalType, List<Type>>();
    for (int i = 0; i < abstractArgTypes.size(); i++) {
      Type abstractType = abstractArgTypes.get(i);
      if (abstractType instanceof UniversalType) {
        // Found a UniversalType. Make sure it's mapped to a list of actual constraints.
        List<Type> actualConstraints = unifications.get(abstractType);
        if (null == actualConstraints) {
          actualConstraints = new ArrayList<Type>();
          unifications.put((UniversalType) abstractType, actualConstraints);
        }

        // Add the actual constraint of the expression being applied as this argument.
        actualConstraints.add(mExprTypes.get(i));
      }
    }

    // Perform unifications on all the UniversalType expressions.
    Map<Type, Type> unificationOut = new HashMap<Type, Type>();
    for (Map.Entry<UniversalType, List<Type>> unification : unifications.entrySet()) {
      UniversalType univType = unification.getKey();
      List<Type> actualConstraints = unification.getValue();
      Type out = univType.getRuntimeType(actualConstraints);
      unificationOut.put(univType, out);
    }
    
    // Finally, generate a list of concrete argument types for coercion purposes.
    for (int i = 0; i < abstractArgTypes.size(); i++ ) {
      Type abstractType = abstractArgTypes.get(i);
      if (abstractType instanceof UniversalType) {
        // Use the resolved type instead.
        mArgTypes[i] = unificationOut.get(abstractType);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Resolved arg[" + i + "] type of " + mFunctionName + " from "
              + abstractType + " to " + mArgTypes[i]);
        }
      } else {
        // Use the specified literal type from the function definition.
        mArgTypes[i] = abstractType;
      }

      assert(mArgTypes[i] != null);
    }

    // Also set mReturnType; if this referenced a UniversalType, use the resolved
    // version. Otherwise, use the version from the function directly.
    Type fnRetType = mFnSymbol.getReturnType();
    if (fnRetType instanceof UniversalType) {
      mReturnType = unificationOut.get(fnRetType);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Resolved return type of " + mFunctionName + " from " + fnRetType
            + " to " + mReturnType);
      }
      if (null == mReturnType) {
        // We can only resolve against our arguments, not our caller's type.
        // This fails for being too abstract.
        throw new TypeCheckException("Output type of function " + mFunctionName
            + " is an unresolved UniversalType: " + fnRetType);
      }
    } else {
      // Normal type; use directly.
      mReturnType = fnRetType;
    }

    mExecFunc = mFnSymbol.getFuncInstance();
    mAutoPromote = mExecFunc.autoPromoteArguments();
    mPartialResults = new Object[mExprTypes.size()];
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
    assert(mArgExprs.size() == mArgTypes.length);
    assert(mExprTypes.size() == mArgTypes.length);

    // Evaluate arguments left-to-right.
    for (int i = 0; i < mArgExprs.size(); i++) {
      Object result = mArgExprs.get(i).eval(e);
      if (mAutoPromote) {
        mPartialResults[i] = coerce(result, mExprTypes.get(i), mArgTypes[i]);
      } else {
        mPartialResults[i] = result;
      }
    }

    try {
      return mExecFunc.eval(mPartialResults);
    } catch (EvalException ee) {
      throw new IOException(ee);
    }
  }

  @Override
  public Type getResolvedType() {
    return mReturnType;
  }

}
