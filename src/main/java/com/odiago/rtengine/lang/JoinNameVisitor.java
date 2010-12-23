// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.List;

import com.odiago.rtengine.exec.AssignedSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.BinOp;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.IdentifierExpr;
import com.odiago.rtengine.parser.JoinedSource;

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
