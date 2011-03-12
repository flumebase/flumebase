// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.lang;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.FnCallExpr;
import com.odiago.flumebase.parser.GroupBy;
import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.WindowDef;

/**
 * Identify the aggregate functions being selected by a query.
 * Add these to the set of AliasedExpr instances that are handled by
 * the aggregation phase rather than eval-exprs phase of a select plan.
 *
 * <p>We currently do not assign an anonymous name to the output of an
 * aggregate fn, so we cannot "SELECT 1 + COUNT(x)"; throw a visitor
 * error if we detect a broader expression surrounding an aggregate
 * function.</p>
 *
 * <p>TODO: Lift that restriction.</p>
 */

public class IdentifyAggregates extends TreeWalkVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(
      IdentifyAggregates.class.getName());

  private List<AliasedExpr> mAggregateExprs;

  // If nonzero, aggregate functions are not allowed at this point. Integer is used
  // as a counting semaphore for nesting depth.
  private int mDisallowAggregates = 0;

  @Override
  protected void visit(SelectStmt s) throws VisitException {

    // Visit the selected columns, add them to the aggregate expression list.
    mAggregateExprs = new ArrayList<AliasedExpr>();
    List<AliasedExpr> exprs = s.getSelectExprs();
    for (AliasedExpr ae: exprs) {
      ae.accept(this);
    }
    // Attach the compiled list of aggregate expressions to the select stmt.
    s.setAggregateExprs(mAggregateExprs);

    // If there are aggregate functions used, make sure we have a window
    // to operate over.
    if (mAggregateExprs.size() > 0 && s.getWindowOver() == null) {
      throw new VisitException("Use of aggregate expressions requires an OVER clause.");
    }

    mAggregateExprs = null;

    // Now visit the where clause; no aggregates allowed there.
    mDisallowAggregates++;
    Expr where = s.getWhereConditions();
    if (null != where) {
      where.accept(this);
    }
    mDisallowAggregates--;


    // Visit the rest of the SELECT statement, including its upstream
    // sources (e.g., nested SELECT statements).
    s.getSource().accept(this);
    GroupBy groupBy = s.getGroupBy();
    if (null != groupBy) {
      groupBy.accept(this);
    }

    List<WindowDef> windowDefs = s.getWindowDefs();
    if (null != windowDefs) {
      for (WindowDef def : windowDefs) {
        def.accept(this);
      }
    }
  }

  @Override
  protected void visit(AliasedExpr e) throws VisitException {
    // Check if the underlying expression is a function call to an aggregate fn.

    Expr subExpr = e.getExpr();
    boolean disallowedInChildren = false;
    if (subExpr instanceof FnCallExpr) {
      FnCallExpr fnCall = (FnCallExpr) subExpr;
      if (fnCall.isAggregate()) {
        // Yep, it's an aggregate function. Mark this aliasedExpr for aggregation step.
        mAggregateExprs.add(e);
        LOG.debug("Identified call to aggregate expr " + fnCall.getFunctionName());
      }
    } else {
      // Don't allow aggregate functions in any other subexpr of 'e'.
      mDisallowAggregates++;
      disallowedInChildren = true;
    }

    try {
      super.visit(e);
    } finally {
      if (disallowedInChildren) {
        // Undo our previous increment.
        mDisallowAggregates--;
      }
    }
  }

  @Override
  protected void visit(FnCallExpr e) throws VisitException {
    if (e.isAggregate() && mDisallowAggregates > 0) {
      throw new VisitException("Encountered call to aggregate function " + e.getFunctionName()
          + " in expression where aggregates are not allowed.");
    }

    // Don't allow calls to aggregate functions in argument expressions
    // (descendent nodes of this function call).
    mDisallowAggregates++;
    try {
      super.visit(e);
    } finally {
      mDisallowAggregates--;
    }
  }
}
