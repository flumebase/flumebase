// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.ConstExpr;
import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.DescribeStmt;
import com.odiago.rtengine.parser.DropStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.IdentifierExpr;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.ShowStmt;
import com.odiago.rtengine.parser.UnaryExpr;

/**
 * Interface implemented by all visitor-pattern AST actors.
 */
public abstract class Visitor {

  public void visit(SQLStatement stmt) throws VisitException {
    if (stmt instanceof CreateStreamStmt) {
      visit((CreateStreamStmt) stmt);
    } else if (stmt instanceof LiteralSource) {
      visit((LiteralSource) stmt);
    } else if (stmt instanceof SelectStmt) {
      visit((SelectStmt) stmt);
    } else if (stmt instanceof ExplainStmt) {
      visit((ExplainStmt) stmt);
    } else if (stmt instanceof DescribeStmt) {
      visit((DescribeStmt) stmt);
    } else if (stmt instanceof ShowStmt) {
      visit((ShowStmt) stmt);
    } else if (stmt instanceof DropStmt) {
      visit((DropStmt) stmt);
    } else if (stmt instanceof ConstExpr) {
      visit((ConstExpr) stmt);
    } else if (stmt instanceof BinExpr) {
      visit((BinExpr) stmt);
    } else if (stmt instanceof FnCallExpr) {
      visit((FnCallExpr) stmt);
    } else if (stmt instanceof IdentifierExpr) {
      visit((IdentifierExpr) stmt);
    } else if (stmt instanceof UnaryExpr) {
      visit((UnaryExpr) stmt);
    } else {
      throw new VisitException("No visit() method for type: " + stmt.getClass().getName()
          + " in class: " + getClass().getName());
    }
  }

  protected void visit(CreateStreamStmt s) throws VisitException {
  }

  protected void visit(LiteralSource s) throws VisitException {
  }

  protected void visit(SelectStmt s) throws VisitException {
  }

  protected void visit(ExplainStmt s) throws VisitException {
  }

  protected void visit(DescribeStmt s) throws VisitException {
  }

  protected void visit(ShowStmt s) throws VisitException {
  }

  protected void visit(DropStmt s) throws VisitException {
  }

  protected void visit(ConstExpr e) throws VisitException {
  }

  protected void visit(BinExpr e) throws VisitException {
  }

  protected void visit(FnCallExpr e) throws VisitException {
  }

  protected void visit(IdentifierExpr e) throws VisitException {
  }

  protected void visit(UnaryExpr e) throws VisitException {
  }
}
