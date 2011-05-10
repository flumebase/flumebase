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

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.AllFieldsExpr;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.IdentifierExpr;

import com.odiago.flumebase.util.StringUtils;

/**
 * Visitor implementation that identifies expressions which will be propagated
 * as fields of a SELECT'ed record, and assigns the unique avro label to each
 * that identifies the field.
 *
 * This visitor sets:
 * <ul>
 * <li>The display label (use the projected label if non-null; field name if its
 * a field, or expr.toStringOneLine() as a last resort)</li>
 * <li>The avro label before projection (identifier name if available, or a
 * generated one otherwise.)</li>
 * <li>The projected label, for IdentifierExprs with null projectedLabel, is set
 * to the identifier name.</li>
 * </ul>
 */
public class AssignFieldLabelsVisitor extends TreeWalkVisitor {

  /** next id number to assign to an AliasedExpr in the query. */
  int mNextId;
  
  public AssignFieldLabelsVisitor() {
    mNextId = 0;
  }

  /**
   * Set the displayLabel for this expression.
   */
  private void setDisplayLabel(AliasedExpr ae) {
    if (ae.getUserAlias() != null) {
      // non-null projected label; the user wants to see this.
      ae.setDisplayLabel(ae.getUserAlias());
    } else if (ae.getExpr() instanceof IdentifierExpr) {
      // Use the field name.
      ae.setDisplayLabel(((IdentifierExpr) ae.getExpr()).getIdentifier());
    } else {
      // Use the expression name.
      ae.setDisplayLabel(ae.getExpr().toStringOneLine());
    }
  }
  
  /**
   * Set the avro label by which we refer to the result of this expression.
   */
  private void setAvroLabel(AliasedExpr ae) {
    Expr e = ae.getExpr();
    if (e instanceof IdentifierExpr) {
      //  Delay setting avro labels for IdentifierExprs; the type checking
      //  phase will provide us with source labels that are set as the
      //  avro labels for the encompassing AliasedExprs.
      //  The exception are '#idents' which are either magic keys or attributes.
      IdentifierExpr ident = (IdentifierExpr) e;
      if (ident.getIdentifier().startsWith("#")) {
        // Attribute, etc. Use _foo instead.
        String label = "_" + ident.getIdentifier().substring(1);
        ae.setAvroLabel(label);
      }
    } else {
      // Use a generated name. Use "__e_" for "[e]xpression".
      String label = "__e_" + mNextId + "_";
      mNextId++;
      ae.setAvroLabel(label);
    }
  }

  /**
   * Set the user-accessible alias for identifier expressions that project
   * to themselves.
   */
  private void setUserAlias(AliasedExpr ae) {
    // IdentifierExprs use their own name as the user alias.
    if (ae.getExpr() instanceof IdentifierExpr && ae.getUserAlias() == null) {
      ae.setUserAlias(StringUtils.dequalify(
          ((IdentifierExpr) ae.getExpr()).getIdentifier()));
    } else if (!(ae.getExpr() instanceof AllFieldsExpr) && ae.getUserAlias() == null) {
      // Make up an alias for this field, since the user didn't set one.
      ae.setUserAlias(ae.getAvroLabel());
    }
  }

  @Override
  public void visit(AliasedExpr ae) {
    setDisplayLabel(ae);
    setAvroLabel(ae);
    setUserAlias(ae);
  }
}
