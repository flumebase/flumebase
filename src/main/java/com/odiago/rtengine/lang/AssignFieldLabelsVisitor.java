// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.AllFieldsExpr;
import com.odiago.rtengine.parser.IdentifierExpr;

import com.odiago.rtengine.util.StringUtils;

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
    //  Delay setting avro labels for IdentifierExprs; the type checking
    //  phase will provide us with source labels that are set as the
    //  avro labels for the encompassing AliasedExprs.
    if (!(ae.getExpr() instanceof IdentifierExpr)) {
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
