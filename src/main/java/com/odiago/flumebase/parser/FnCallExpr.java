/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.parser;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.Bucket;
import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FnSymbol;
import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.AggregateFunc;
import com.odiago.flumebase.lang.EvalException;
import com.odiago.flumebase.lang.Function;
import com.odiago.flumebase.lang.ListType;
import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;
import com.odiago.flumebase.lang.TypeCheckException;
import com.odiago.flumebase.lang.UniversalConstraintExtractor;
import com.odiago.flumebase.lang.UniversalType;

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
  private Function mExecFunc; // The function instance to execute.
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

  public void setArgExpressions(List<Expr> exprs) {
    mArgExprs = exprs;
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

    fnSymbol = fnSymbol.resolveAliases();

    if (!(fnSymbol instanceof FnSymbol)) {
      // This symbol isn't a function call?
      throw new TypeCheckException("Symbol " + mFunctionName + " is not a function");
    }

    mFnSymbol = (FnSymbol) fnSymbol; // memoize this for later.

    // Get a list of argument types from the function symbol. These may include
    // universal types we need to concretize.
    List<Type> abstractArgTypes = new ArrayList<Type>(mFnSymbol.getArgumentTypes());

    // Argument types for varargs, after the fixed args.
    List<Type> abstractVarArgTypes = mFnSymbol.getVarArgTypes();

    // Check that arity matches.
    int argsRemaining = mArgExprs.size();
    argsRemaining -= abstractArgTypes.size();
    if (argsRemaining < 0 || (argsRemaining > 0 && abstractVarArgTypes.size() == 0)) {
      // Too few actual args, or too many args (and this is not a varargs fn).
      throw new TypeCheckException("Function " + mFunctionName + " requires "
          + abstractArgTypes.size() + " arguments, but received " + mArgExprs.size());
    }

    if (argsRemaining > 0 && abstractVarArgTypes.size() > 0) {
      // varargs may need to come in pairs, etc. Check that we have a correct multiple
      // of the number of varargs available.
      int argRemainder = argsRemaining % abstractVarArgTypes.size();
      if (0 != argRemainder) {
        throw new TypeCheckException("Function " + mFunctionName + " requires varargs "
            + "in sets of " + abstractVarArgTypes.size() + ", but this call has "
            + argRemainder + " too few.");
      }
    }

    // For each actual vararg, add its type to the abstractArgTypes list.
    if (abstractVarArgTypes.size() > 0) {
      int numVarArgSets = (mArgExprs.size() - abstractArgTypes.size())
          / abstractVarArgTypes.size();
      for (int i = 0; i < numVarArgSets; i++) {
        abstractArgTypes.addAll(abstractVarArgTypes);
      }
    }

    assert mArgExprs.size() == abstractArgTypes.size();

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

    mArgTypes = new Type[mArgExprs.size()];

    // Now identify all the UniversalType instances in here, and the
    // actual constraints on each of these.
    Map<UniversalType, List<Type>> unifications = new HashMap<UniversalType, List<Type>>();
    for (int i = 0; i < abstractArgTypes.size(); i++) {
      Type abstractType = abstractArgTypes.get(i);
      Type actualType = mExprTypes.get(i);
      UniversalConstraintExtractor constraintExtractor = new UniversalConstraintExtractor();
      if (constraintExtractor.extractConstraint(abstractType, actualType)) {
        // Found a UniversalType. Make sure it's mapped to a list of actual constraints.
        UniversalType univType = constraintExtractor.getUniversalType();
        List<Type> actualConstraints = unifications.get(univType);
        if (null == actualConstraints) {
          actualConstraints = new ArrayList<Type>();
          unifications.put(univType, actualConstraints);
        }

        // Add the actual constraint of the expression being applied as this argument.
        actualConstraints.add(constraintExtractor.getConstraintType());
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
      mArgTypes[i] = abstractType.replaceUniversal(unificationOut);
      assert mArgTypes[i] != null;
    }

    // Also set mReturnType; if this referenced a UniversalType, use the resolved
    // version. Otherwise, use the version from the function directly.
    Type fnRetType = mFnSymbol.getReturnType();
    try {
      mReturnType = fnRetType.replaceUniversal(unificationOut);
    } catch (TypeCheckException tce) {
      // We can only resolve against our arguments, not our caller's type.
      if (fnRetType instanceof ListType) {
        // If the unresolved typevar is an argument to a list type, we can
        // return this -- it's going to be an empty list, so we can return
        // LIST<ANY>
        // TODO(aaron): This allows to_list() to produce an empty list, but
        // putting this check here feels a bit like a hack to me.
        mReturnType = new ListType(Type.getNullable(Type.TypeName.ANY));
      } else {
        // This fails for being too abstract.
        throw new TypeCheckException("Output type of function " + mFunctionName
            + " is an unresolved UniversalType: " + fnRetType, tce);
      }
    }

    assert null != mReturnType;

    mExecFunc = mFnSymbol.getFuncInstance();
    mAutoPromote = mExecFunc.autoPromoteArguments();
    mPartialResults = new Object[mExprTypes.size()];
  }

  /** @return true if this fn call is an aggregate function. */
  public boolean isAggregate() {
    return mExecFunc instanceof AggregateFunc;
  }

  /** @return true if this fn call is a scalar function. */
  public boolean isScalar() {
    return mExecFunc instanceof ScalarFunc;
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    List<TypedField> out = new ArrayList<TypedField>();
    for (Expr e : mArgExprs) {
      out.addAll(e.getRequiredFields(symTab));
    }

    return out;
  }

  /**
   * Evaluate all the arguments to the function in preparation for
   * calling the function on them. Stores its output in the
   * mPartialResults array.
   */
  private void evaluateArguments(EventWrapper e) throws IOException {
    // Ensure that we have actual and specified types for all actual expression arguments.
    // We may have some extras, if virtual arguments were used to finish type unification.
    assert mArgExprs.size() <= mArgTypes.length;
    assert mArgExprs.size() <= mExprTypes.size();

    // Evaluate arguments left-to-right.
    for (int i = 0; i < mArgExprs.size(); i++) {
      Object result = mArgExprs.get(i).eval(e);
      if (mAutoPromote) {
        mPartialResults[i] = coerce(result, mExprTypes.get(i), mArgTypes[i]);
      } else {
        mPartialResults[i] = result;
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public Object eval(EventWrapper e) throws IOException {
    assert mExecFunc instanceof ScalarFunc;
    evaluateArguments(e);

    try {
      return ((ScalarFunc) mExecFunc).eval(e, mPartialResults);
    } catch (EvalException ee) {
      throw new IOException(ee);
    }
  }

  /**
   * For a function call representing an aggregation function, call the
   * bucket-insertion method of the AggregationFunc on the (already
   * evaluated) arguments of this function.
   */
  public <T> void insertAggregate(EventWrapper e, Bucket<T> bucket) throws IOException {
    assert mExecFunc instanceof AggregateFunc;
    evaluateArguments(e);
    
    try {
      ((AggregateFunc<T>) mExecFunc).addToBucket(mPartialResults[0], bucket, mReturnType);
    } catch (EvalException ee) {
      throw new IOException(ee);
    }
  }

  /**
   * For a function call representing an aggregation function, call the
   * finishWindow method of the AggregationFunc on the set of buckets
   * comprising the time window we are aggregating over.
   */
  public <T> Object finishWindow(Iterable<Bucket<T>> buckets) throws IOException {
    assert mExecFunc instanceof AggregateFunc;

    try {
      return ((AggregateFunc<T>) mExecFunc).finishWindow(buckets, mReturnType);
    } catch (EvalException ee) {
      throw new IOException(ee);
    }
  }

  @Override
  public Type getResolvedType() {
    return mReturnType;
  }

  @Override
  public boolean isConstant() {
    return false;
  }
}
