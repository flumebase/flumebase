// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.List;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.UnaryExpr;

/**
 * TreeWalkVisitor comes with default visit() methods that visit all children of a given
 * node. Subclasses should call super.visit() when they want recursion to occur.
 */
public abstract class TreeWalkVisitor extends Visitor {

  protected void visit(CreateStreamStmt s) throws VisitException {
    s.getFormatSpec().accept(this);
  }

  protected void visit(SelectStmt s) throws VisitException {
    List<AliasedExpr> exprs = s.getSelectExprs();
    for (AliasedExpr e : exprs) {
      e.accept(this);
    }
    s.getSource().accept(this);
    // TODO(aaron): WhereConditions?
  }

  protected void visit(ExplainStmt s) throws VisitException {
    s.getChildStmt().accept(this);
  }

  protected void visit(BinExpr e) throws VisitException {
    e.getLeftExpr().accept(this);
    e.getRightExpr().accept(this);
  }

  protected void visit(FnCallExpr e) throws VisitException {
    // TODO(aaron): Implement this
    throw new VisitException("Don't know how to treewalk a fn call.");
  }

  protected void visit(UnaryExpr e) throws VisitException {
    e.getSubExpr().accept(this);
  }

  protected void visit(AliasedExpr e) throws VisitException {
    e.getExpr().accept(this);
  }
}

