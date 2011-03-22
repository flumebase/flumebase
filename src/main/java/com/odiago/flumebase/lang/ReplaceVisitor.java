/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.lang;

import java.util.List;

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.BinExpr;
import com.odiago.flumebase.parser.CreateStreamStmt;
import com.odiago.flumebase.parser.ExplainStmt;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.FnCallExpr;
import com.odiago.flumebase.parser.FormatSpec;
import com.odiago.flumebase.parser.GroupBy;
import com.odiago.flumebase.parser.JoinedSource;
import com.odiago.flumebase.parser.RangeSpec;
import com.odiago.flumebase.parser.RecordSource;
import com.odiago.flumebase.parser.SQLStatement;
import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.UnaryExpr;
import com.odiago.flumebase.parser.WindowDef;
import com.odiago.flumebase.parser.WindowSpec;

/**
 * Replaces all instances of oldChild with newChild in the AST being visited.
 */
public class ReplaceVisitor extends TreeWalkVisitor {
  /** The item to be replaced. */
  private SQLStatement mOldChild;

  /** The item to replace it with. */
  private SQLStatement mNewChild;

  public ReplaceVisitor(SQLStatement oldChild, SQLStatement newChild) {
    mOldChild = oldChild;
    mNewChild = newChild;
  }

  @Override
  protected void visit(CreateStreamStmt s) throws VisitException {
    SQLStatement child = s.getFormatSpec();
    if (mOldChild == child) {
      s.setFormatSpec((FormatSpec) mNewChild);
    }

    super.visit(s);
  }

  @Override
  protected void visit(SelectStmt s) throws VisitException {
    List<AliasedExpr> exprs = s.getSelectExprs();
    for (int i = 0; i < exprs.size(); i++) {
      if (mOldChild == exprs.get(i)) {
        exprs.set(i, (AliasedExpr) mNewChild);
      }
    }

    SQLStatement src = s.getSource();
    if (mOldChild == src) {
      s.setSource(mNewChild);
    }

    Expr where = s.getWhereConditions();
    if (mOldChild == where) {
      s.setWhereConditions((Expr) mNewChild);
    }

    GroupBy groupBy = s.getGroupBy();
    if (mOldChild == groupBy) {
      s.setGroupBy((GroupBy) mNewChild);
    }

    Expr aggregateOver = s.getWindowOver();
    if (mOldChild == aggregateOver) {
      s.setWindowOver((Expr) mNewChild);
    }

    List<WindowDef> windowDefs = s.getWindowDefs();
    if (null != windowDefs) {
      for (int i = 0; i < windowDefs.size(); i++) {
        if (mOldChild == windowDefs.get(i)) {
          windowDefs.set(i, (WindowDef) mNewChild);
        }
      }
    }

    super.visit(s);
  }

  @Override
  protected void visit(ExplainStmt s) throws VisitException {
    SQLStatement child = s.getChildStmt();
    if (mOldChild == child) {
      s.setChildStmt(mNewChild);
    }

    super.visit(s);
  }

  @Override
  protected void visit(BinExpr e) throws VisitException {
    Expr left = e.getLeftExpr();
    if (mOldChild == left) {
      e.setLeftExpr((Expr) mNewChild);
    }

    Expr right = e.getRightExpr();
    if (mOldChild == right) {
      e.setRightExpr((Expr) mNewChild);
    }

    super.visit(e);
  }

  @Override
  protected void visit(FnCallExpr e) throws VisitException {
    List<Expr> args = e.getArgExpressions();
    if (null != args) {
      for (int i = 0; i < args.size(); i++) {
        if (mOldChild == args.get(i)) {
          args.set(i, (Expr) mNewChild);
        }
      }
    }

    super.visit(e);
  }

  @Override
  protected void visit(UnaryExpr e) throws VisitException {
    Expr child = e.getSubExpr();
    if (mOldChild == child) {
      e.setSubExpr((Expr) mNewChild);
    }

    super.visit(e);
  }

  @Override
  protected void visit(AliasedExpr e) throws VisitException {
    Expr child = e.getExpr();
    if (mOldChild == child) {
      e.setExpr((Expr) mNewChild);
    }
    super.visit(e);
  }

  @Override
  protected void visit(WindowDef e) throws VisitException {
    WindowSpec spec = e.getWindowSpec();
    if (mOldChild == spec) {
      e.setWindowSpec((WindowSpec) mNewChild);
    }

    super.visit(e);
  }

  @Override
  protected void visit(WindowSpec e) throws VisitException {
    RangeSpec range = e.getRangeSpec();
    if (mOldChild == range) {
      e.setRangeSpec((RangeSpec) mNewChild);
    }
    super.visit(e);
  }

  @Override
  protected void visit(RangeSpec e) throws VisitException {
    Expr prev = e.getPrevSize();
    Expr after = e.getAfterSize();

    if (mOldChild == prev) {
      e.setPrevSize((Expr) mNewChild);
    }

    if (mOldChild == after) {
      e.setAfterSize((Expr) mNewChild);
    }

    super.visit(e);
  }

  @Override
  protected void visit(JoinedSource s) throws VisitException {
    RecordSource left = s.getLeft();
    RecordSource right = s.getRight();
    Expr join = s.getJoinExpr();
    Expr window = s.getWindowExpr();

    if (mOldChild == left) {
      s.setLeft((RecordSource) mNewChild);
    }

    if (mOldChild == right) {
      s.setRight((RecordSource) mNewChild);
    }

    if (mOldChild == join) {
      s.setJoinExpr((Expr) mNewChild);
    }

    if (mOldChild == window) {
      s.setWindowExpr((Expr) mNewChild);
    }

    super.visit(s);
  }
}
