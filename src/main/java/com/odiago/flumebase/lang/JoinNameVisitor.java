// (c) Copyright 2010 Odiago, Inc.

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
