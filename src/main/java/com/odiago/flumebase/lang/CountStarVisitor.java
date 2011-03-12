// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.lang;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.parser.AllFieldsExpr;
import com.odiago.flumebase.parser.ConstExpr;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.FnCallExpr;

/**
 * Syntactically replace COUNT(*) with COUNT(1) in the SELECT expressions;
 * we can't typecheck a '*' inside an expression, but an arbitrary constant
 * expression will suffice as an argument here.
 */

public class CountStarVisitor extends TreeWalkVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(
      CountStarVisitor.class.getName());

  @Override
  protected void visit(FnCallExpr e) throws VisitException {
    if (e.getFunctionName().equals("count")) {
      // Yep, it's a COUNT() function. Check if its argument is a '*'.
      List<Expr> fnArgs = e.getArgExpressions();
      if (fnArgs.size() == 1 && fnArgs.get(0) instanceof AllFieldsExpr) {
        // This matches COUNT(*).
        LOG.debug("Replaced count(*) in SELECT statement.");
        e.setArgExpressions(Collections.singletonList((Expr)
            new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf(1))));
      }
    }
  }
}
