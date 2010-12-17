// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.AllFieldsExpr;
import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.ConstExpr;
import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.DescribeStmt;
import com.odiago.rtengine.parser.DropStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.FormatSpec;
import com.odiago.rtengine.parser.IdentifierExpr;
import com.odiago.rtengine.parser.JoinedSource;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.RangeSpec;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.ShowStmt;
import com.odiago.rtengine.parser.UnaryExpr;
import com.odiago.rtengine.parser.WindowDef;
import com.odiago.rtengine.parser.WindowSpec;

/**
 * Interface implemented by all visitor-pattern AST actors.
 */
public abstract class Visitor {
  private static final Logger LOG = LoggerFactory.getLogger(Visitor.class.getName());

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
    } else if (stmt instanceof AllFieldsExpr) {
      visit((AllFieldsExpr) stmt);
    } else if (stmt instanceof AliasedExpr) {
      visit((AliasedExpr) stmt);
    } else if (stmt instanceof FormatSpec) {
      visit((FormatSpec) stmt);
    } else if (stmt instanceof JoinedSource) {
      visit((JoinedSource) stmt);
    } else if (stmt instanceof RangeSpec) {
      visit((RangeSpec) stmt);
    } else if (stmt instanceof WindowDef) {
      visit((WindowDef) stmt);
    } else if (stmt instanceof WindowSpec) {
      visit((WindowSpec) stmt);
    } else {
      throw new VisitException("No visit() method for type: " + stmt.getClass().getName()
          + " in class: " + getClass().getName());
    }
  }
  
  private void warnEmptyVisit(SQLStatement stmt) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Using default (empty) visit() method for visitor type=" + getClass().getName()
          + " and argument type=" + stmt.getClass().getName());
    }
  }

  protected void visit(CreateStreamStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(LiteralSource s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(SelectStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(ExplainStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(DescribeStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(ShowStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(DropStmt s) throws VisitException {
    warnEmptyVisit(s);
  }

  protected void visit(ConstExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(BinExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(FnCallExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(IdentifierExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(UnaryExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(AllFieldsExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(AliasedExpr e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(FormatSpec e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(JoinedSource e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(RangeSpec e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(WindowDef e) throws VisitException {
    warnEmptyVisit(e);
  }

  protected void visit(WindowSpec e) throws VisitException {
    warnEmptyVisit(e);
  }
}

