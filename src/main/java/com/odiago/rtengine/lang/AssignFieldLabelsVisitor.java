// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.IdentifierExpr;

/**
 * Visitor implementation that identifies expressions which will be propagated
 * as fields of a SELECT'ed record, and assigns the unique avro label to each
 * that identifies the field. Also assigns the user-readable label for the field.
 */
public class AssignFieldLabelsVisitor extends TreeWalkVisitor {

  /** next id number to assign to an AliasedExpr in the query. */
  int mNextId;
  
  public AssignFieldLabelsVisitor() {
    mNextId = 0;
  }

  @Override
  public void visit(AliasedExpr ae) {
    String label;

    if (ae.getUserLabel() != null) {
      // The user has specified the label (with an 'AS' clause).
      // This is also therefore the avro label.
      label = ae.getUserLabel();
    } else if (ae.getExpr() instanceof IdentifierExpr) {
      // This is just a direct field reference -- use that name by itself.
      label = ((IdentifierExpr) ae.getExpr()).getIdentifier();
      ae.setUserLabel(label);
    } else {
      label = "__f_" + mNextId + "_";
      mNextId++;
      ae.setUserLabel(ae.getExpr().toStringOneLine());
    }

    ae.setAvroLabel(label);

  }
}
