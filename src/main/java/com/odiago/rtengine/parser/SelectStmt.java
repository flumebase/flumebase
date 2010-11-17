// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.plan.ConsoleOutputNode;
import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PlanNode;
import com.odiago.rtengine.plan.StrMatchFilterNode;

/**
 * SELECT statement.
 */
public class SelectStmt extends SQLStatement {

  private static final Logger LOG = LoggerFactory.getLogger(SelectStmt.class.getName());

  private FieldList mFields;
  // Source stream for the FROM clause. must be a LiteralSource or a SelectStmt.
  // (That fact is proven by a TypeChecker visitor.)
  private SQLStatement mSource;
  private WhereConditions mWhere;

  public SelectStmt(FieldList fields, SQLStatement source, WhereConditions where) {
    mFields = fields;
    mSource = source;
    mWhere = where;
  }

  public FieldList getFields() {
    return mFields;
  }

  public SQLStatement getSource() {
    return mSource;
  }

  public WhereConditions getWhereConditions() {
    return mWhere;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("SELECT");
    sb.append("\n");
    pad(sb, depth);
    sb.append("fields:\n");
    if (mFields.isAllFields()) {
      pad(sb, depth + 1);
      sb.append("(all)\n");
    } else {
      for (String fieldName : mFields) {
        pad(sb, depth + 1);
        sb.append(fieldName);
        sb.append("\n");
      }
    }
    pad(sb, depth);
    sb.append("FROM:\n");
    mSource.format(sb, depth + 1);
    if (null != mWhere) {
      pad(sb, depth);
      sb.append("WHERE:\n");
      pad(sb, depth + 1);
      sb.append(mWhere.getText());
      sb.append("\n");
    }
  }

  @Override
  public void createExecPlan(PlanContext planContext) {
    SQLStatement source = getSource();
    // Create an execution plan to build the source (it may be a single node
    // representing a Flume source or file, or it may be an entire DAG because
    // we use another SELECT statement as a source) inside a new context.
    PlanContext childContext = new PlanContext(planContext.getMsgBuilder(),
        new FlowSpecification(), false);
    source.createExecPlan(childContext);

    // Now incorporate that entire plan into our plan.
    FlowSpecification flowSpec = planContext.getFlowSpec();
    flowSpec.addNodesFromDAG(childContext.getFlowSpec());

    // TODO(aaron): Add a projection level that grabs only the fields we care about.
    FieldList fields = getFields();
    LOG.warn("Do not know how to project to field list: " + fields);
  
    WhereConditions w = getWhereConditions();
    if (w != null) {
      // Non-null filter conditions; apply the filter to all of our sources.
      String filterText = unquote(w.getText());
      PlanNode filterNode = new StrMatchFilterNode(filterText);
      flowSpec.attachToLastLayer(filterNode);
    }

    if (planContext.isRoot()) {
      // SELECT statements that are root queries need to go to the console.
      flowSpec.attachToLastLayer(new ConsoleOutputNode());
    }
  }
}

