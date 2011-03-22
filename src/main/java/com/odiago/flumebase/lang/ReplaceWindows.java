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

import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.WindowSymbol;

import com.odiago.flumebase.parser.IdentifierExpr;
import com.odiago.flumebase.parser.SQLStatement;
import com.odiago.flumebase.parser.WindowSpec;

/**
 * SELECT statements may contain named WINDOW definitions which are
 * referenced as identifiers elsewhere in the statement. An identifier,
 * when evaluated, will attempt to read the named field from the
 * event being processed. WindowSymbol instances are pointers to the
 * WindowSpec they define; such IdentifierExpr instances should be
 * replaced by the WindowSpec instances they reference.
 */
public class ReplaceWindows extends TreeWalkVisitor {

  /** 
   * When we find an IdentifierExpr that should be replaced by a WindowSpec,
   * this is filled with the WindowSpec to replace it with. After visiting
   * the IdentifierExpr, we do the replacement and set this field to null.
   */
  private WindowSpec mReplaceWith;
  
  @Override
  public void after(SQLStatement parent, SQLStatement child) throws VisitException {
    if (null != mReplaceWith) {
      // Do the replacement.
      replace(parent, child, mReplaceWith);
      mReplaceWith = null;
    }
  }

  @Override
  public void visit(IdentifierExpr ident) {
    AssignedSymbol assignedSym = ident.getAssignedSymbol();

    if (assignedSym instanceof WindowSymbol) {
      // We have found an IdentifierExpr that should be replaced.
      WindowSymbol windowSym = (WindowSymbol) assignedSym;
      mReplaceWith = windowSym.getWindowSpec();
    }
  }
}
