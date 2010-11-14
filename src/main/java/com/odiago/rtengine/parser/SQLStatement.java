// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import com.odiago.rtengine.lang.VisitException;
import com.odiago.rtengine.lang.Visitor;

import com.odiago.rtengine.plan.PlanContext;

/**
 * Abstract base class for statements in the SQL statement AST
 */
public abstract class SQLStatement {
  /**
   * Format the contents of this AST into the provided StringBuilder,
   * starting with indentation depth 'depth'.
   */
  public abstract void format(StringBuilder sb, int depth);
  
  public void format(StringBuilder sb) {
    format(sb, 0);
  }

  /**
   * Format the contents of this AST to a string.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    format(sb);
    return sb.toString();
  }

  /**
   * Facilitate the visitor pattern over this AST.
   */
  public void accept(Visitor v) throws VisitException {
    v.visit(this);
  }

  /**
   * Add indentation to the current line of the string builder
   * for the current AST depth.
   */
  protected void pad(StringBuilder sb, int depth) {
    for (int i = 0; i < depth; i++) {
      sb.append("  ");
    }
  }

  /**
   * Transform this AST element into a set of DAG elements for the execution plan.
   */
  public void createExecPlan(PlanContext planContext) {
    throw new RuntimeException("This node type cannot be incorporated into an execution plan.");
  }

  /**
   * Remove "double quotes" from around a string, if present.
   */
  protected final String unquote(String srcName) {
    if (srcName.startsWith("\"") && srcName.endsWith("\"")) {
      return srcName.substring(1, srcName.length() - 1);
    } else {
      return srcName;
    }
  }
}

