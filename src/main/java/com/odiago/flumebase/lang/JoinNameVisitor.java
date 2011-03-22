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

package com.odiago.flumebase.lang;

import java.util.List;

import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.parser.BinExpr;
import com.odiago.flumebase.parser.BinOp;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.IdentifierExpr;
import com.odiago.flumebase.parser.JoinedSource;

/**
 * Assigns unique names to the output of each JOIN clause.
 */
public class JoinNameVisitor extends TreeWalkVisitor {
  private int mNextId = 0;

  @Override
  protected void visit(JoinedSource src) throws VisitException {
    src.setSourceName("__rtsql_join_" + mNextId + "_");

    // Handle nested joins.
    src.getLeft().accept(this);
    src.getRight().accept(this);
  }
}
