// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import java.util.List;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.GroupBy;
import com.odiago.rtengine.parser.JoinedSource;
import com.odiago.rtengine.parser.RangeSpec;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.UnaryExpr;
import com.odiago.rtengine.parser.WindowDef;
import com.odiago.rtengine.parser.WindowSpec;

/**
 * TreeWalkVisitor comes with default visit() methods that visit all children of a given
 * node. Subclasses should call super.visit() when they want recursion to occur.
 */
public abstract class TreeWalkVisitor extends Visitor {

  @Override
  protected void visit(CreateStreamStmt s) throws VisitException {
    s.getFormatSpec().accept(this);
  }

  @Override
  protected void visit(SelectStmt s) throws VisitException {
    List<AliasedExpr> exprs = s.getSelectExprs();
    for (AliasedExpr e : exprs) {
      e.accept(this);
    }
    s.getSource().accept(this);

    Expr where = s.getWhereConditions();
    if (null != where) {
      where.accept(this);
    }

    GroupBy groupBy = s.getGroupBy();
    if (null != groupBy) {
      groupBy.accept(this);
    }

    Expr aggregateOver = s.getWindowOver();
    if (null != aggregateOver) {
      aggregateOver.accept(this);
    }

    List<WindowDef> windowDefs = s.getWindowDefs();
    if (null != windowDefs) {
      for (WindowDef def : windowDefs) {
        def.accept(this);
      }
    }
  }

  @Override
  protected void visit(ExplainStmt s) throws VisitException {
    s.getChildStmt().accept(this);
  }

  @Override
  protected void visit(BinExpr e) throws VisitException {
    e.getLeftExpr().accept(this);
    e.getRightExpr().accept(this);
  }

  @Override
  protected void visit(FnCallExpr e) throws VisitException {
    List<Expr> args = e.getArgExpressions();
    if (null != args) {
      for (Expr ae : args) {
        ae.accept(this);
      }
    }
  }

  @Override
  protected void visit(UnaryExpr e) throws VisitException {
    e.getSubExpr().accept(this);
  }

  @Override
  protected void visit(AliasedExpr e) throws VisitException {
    e.getExpr().accept(this);
  }

  @Override
  protected void visit(WindowDef e) throws VisitException {
    e.getWindowSpec().accept(this);
  }

  @Override
  protected void visit(WindowSpec e) throws VisitException {
    e.getRangeSpec().accept(this);
  }

  @Override
  protected void visit(RangeSpec e) throws VisitException {
    e.getPrevSize().accept(this);
    e.getAfterSize().accept(this);
  }

  @Override
  protected void visit(JoinedSource s) throws VisitException {
    s.getLeft().accept(this);
    s.getRight().accept(this);
    s.getJoinExpr().accept(this);
    s.getWindowExpr().accept(this);
  }
}

