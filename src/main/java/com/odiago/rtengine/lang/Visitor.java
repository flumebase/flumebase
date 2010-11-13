// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;

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
    } else {
      throw new VisitException("No visit() method for type: " + stmt.getClass().getName());
    }
  }

  protected abstract void visit(CreateStreamStmt s) throws VisitException;
  protected abstract void visit(LiteralSource s) throws VisitException;
  protected abstract void visit(SelectStmt s) throws VisitException;
}
