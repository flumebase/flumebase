// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.IdentifierExpr;

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
    if (ae.getProjectedLabel() != null) {
      // non-null projected label; the user wants to see this.
      ae.setDisplayLabel(ae.getProjectedLabel());
    } else if (ae.getExpr() instanceof IdentifierExpr) {
      // Use the field name.
      ae.setDisplayLabel(((IdentifierExpr) ae.getExpr()).getIdentifier());
    } else {
      // Use the expression name.
      ae.setDisplayLabel(ae.getExpr().toStringOneLine());
    }
  }
  
  /**
   * Set the avro label by which we refer to the result of this expression
   * in pre-projection phases.
   */
  private void setAvroLabel(AliasedExpr ae) {
    if (ae.getExpr() instanceof IdentifierExpr) {
      // Use the field name.
      ae.setAvroLabel(((IdentifierExpr) ae.getExpr()).getIdentifier());
    } else {
      // Use a generated name.
      String label = "__f_" + mNextId + "_";
      mNextId++;
      ae.setAvroLabel(label);
    }
  }

  /**
   * Set the projectedLabel for identifier expressions that project
   * to themselves.
   */
  private void setProjectedLabel(AliasedExpr ae) {
    // IdentifierExprs are projected to themselves.
    if (ae.getExpr() instanceof IdentifierExpr && ae.getProjectedLabel() == null) {
      ae.setProjectedLabel(((IdentifierExpr) ae.getExpr()).getIdentifier());
    }
  }

  @Override
  public void visit(AliasedExpr ae) {
    setDisplayLabel(ae);
    setAvroLabel(ae);
    setProjectedLabel(ae);
  }
}
