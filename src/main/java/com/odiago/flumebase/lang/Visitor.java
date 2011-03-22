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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.AllFieldsExpr;
import com.odiago.flumebase.parser.BinExpr;
import com.odiago.flumebase.parser.ConstExpr;
import com.odiago.flumebase.parser.CreateStreamStmt;
import com.odiago.flumebase.parser.DescribeStmt;
import com.odiago.flumebase.parser.DropStmt;
import com.odiago.flumebase.parser.ExplainStmt;
import com.odiago.flumebase.parser.FnCallExpr;
import com.odiago.flumebase.parser.FormatSpec;
import com.odiago.flumebase.parser.GroupBy;
import com.odiago.flumebase.parser.IdentifierExpr;
import com.odiago.flumebase.parser.JoinedSource;
import com.odiago.flumebase.parser.LiteralSource;
import com.odiago.flumebase.parser.RangeSpec;
import com.odiago.flumebase.parser.SQLStatement;
import com.odiago.flumebase.parser.SelectStmt;
import com.odiago.flumebase.parser.ShowStmt;
import com.odiago.flumebase.parser.UnaryExpr;
import com.odiago.flumebase.parser.WindowDef;
import com.odiago.flumebase.parser.WindowSpec;

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
    } else if (stmt instanceof GroupBy) {
      visit((GroupBy) stmt);
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

  protected void visit(GroupBy g) throws VisitException {
    warnEmptyVisit(g);
  }
}

