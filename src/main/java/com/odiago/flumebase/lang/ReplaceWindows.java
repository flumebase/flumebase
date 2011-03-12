// (c) Copyright 2011 Odiago, Inc.

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
