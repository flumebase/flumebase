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

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.exec.builtins.bin2str;

import com.odiago.flumebase.lang.ListType;
import com.odiago.flumebase.lang.NullableType;
import com.odiago.flumebase.lang.PreciseType;
import com.odiago.flumebase.lang.Type;

/**
 * An expression which evaluates to a value inside a record.
 */
public abstract class Expr extends SQLStatement {
  private static final Logger LOG = LoggerFactory.getLogger(Expr.class.getName());

  private static final bin2str BIN2STR_FN; // For coercing binary -> string
  static {
    BIN2STR_FN = new bin2str();
  }

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
  public abstract Type getResolvedType();


  /**
   * @return true if this expression is constant.
   */
  public abstract boolean isConstant();

  /**
   * @return true if this expr needs to be eval'd (in an EvaluateExprs step),
   * false if it's a "normal" identifier that can be pulled.
   */
  public boolean requiresEval() {
    return true;
  }

  /**
   * @return an object representing the same value as 'val' but coerced
   * from valType into targetType.
   */
  protected Object coerce(Object val, Type valType, Type targetType) {
    LOG.debug("Converting: " + valType + " to " + targetType);
    return targetType.coerceValue(valType, val);
  }
}
