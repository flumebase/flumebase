// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.plan.ConsoleOutputNode;
import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.MemoryOutputNode;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PlanNode;
import com.odiago.rtengine.plan.ProjectionNode;
import com.odiago.rtengine.plan.StrMatchFilterNode;

/**
 * SELECT statement.
 */
public class SelectStmt extends SQLStatement {

  private static final Logger LOG = LoggerFactory.getLogger(SelectStmt.class.getName());

  /**
   * Configuration key that specifies how we should deliver output records of a
   * top-level RTSQL statement to the client. If this is set to "$console," we
   * print to the screen. Other strings cause us to allocate a list buffer that
   * can be retrieved later by the client.
   */
  public static final String CLIENT_SELECT_TARGET_KEY = "rtsql.client.select.target";

  /** Special value for rtsql.client.select.target that prints to stdout. */
  public static final String CONSOLE_SELECT_TARGET = "$console";

  /** The default for rtsql.client.select.target is to use the console. */
  public static final String DEFAULT_CLIENT_SELECT_TARGET = CONSOLE_SELECT_TARGET;

  /** Set of fields or other expressions to select */
  private List<AliasedExpr> mSelectExprs;

  // Source stream for the FROM clause. must be a LiteralSource or a SelectStmt.
  // (That fact is proven by a TypeChecker visitor.)
  private SQLStatement mSource;
  private WhereConditions mWhere;

  public SelectStmt(List<AliasedExpr> selExprs, SQLStatement source, WhereConditions where) {
    mSelectExprs = selExprs;
    mSource = source;
    mWhere = where;
  }

  public List<AliasedExpr> getSelectExprs() {
    return mSelectExprs;
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
    sb.append("expressions:\n");
    for (AliasedExpr ae : mSelectExprs) {
      ae.format(sb, depth + 1);
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
  public PlanContext createExecPlan(PlanContext planContext) {
    SQLStatement source = getSource();
    WhereConditions where = getWhereConditions();

    // Create an execution plan to build the source (it may be a single node
    // representing a Flume source or file, or it may be an entire DAG because
    // we use another SELECT statement as a source) inside a new context.
    PlanContext sourceInCtxt = new PlanContext(planContext);
    sourceInCtxt.setRoot(false);
    sourceInCtxt.setFlowSpec(new FlowSpecification());
    PlanContext sourceOutCtxt = source.createExecPlan(sourceInCtxt);
    SymbolTable srcOutSymbolTable = sourceOutCtxt.getSymbolTable();

    // Now incorporate that entire plan into our plan.
    FlowSpecification flowSpec = planContext.getFlowSpec();
    flowSpec.addNodesFromDAG(sourceOutCtxt.getFlowSpec());

    // Add a projection level that grabs all the fields we care about.
    Schema sourceSchema = sourceOutCtxt.getSchema();
    List<TypedField> allRequiredFields = new ArrayList<TypedField>();

    // Create a list containing the (ordered) set of fields we want emitted to the console.
    List<TypedField> consoleFields = new ArrayList<TypedField>();

    // Start with all the fields the user explicitly selected.
    List<AliasedExpr> exprList = getSelectExprs();
    for (AliasedExpr aliasExpr : exprList) {
      Expr e = aliasExpr.getExpr();
      if (e instanceof AllFieldsExpr) {
        // Use all field names listed as outputs from the source's output context.
        for (TypedField outField : sourceOutCtxt.getOutFields()) {
          consoleFields.add(outField);
          allRequiredFields.add(outField);
        }
      } else {
        // Get the type within the expression, and add the appropriate labels.
        // These have been already assigned by a visitor pass.
        TypedField field = new TypedField(
          aliasExpr.getUserLabel(), e.getType(srcOutSymbolTable),
          aliasExpr.getAvroLabel(), aliasExpr.getUserLabel());
        consoleFields.add(field);

        allRequiredFields.addAll(e.getRequiredFields(srcOutSymbolTable));
      }
    }

    if (null != where) {
      // Add to this all the fields required by the where clause.
      allRequiredFields.addAll(where.getRequiredFields(srcOutSymbolTable));
    }

    // Create the projected schema based on the symbol table returned by our source. 
    allRequiredFields = distinctFields(allRequiredFields);
    Schema projectedSchema = createFieldSchema(allRequiredFields);
    ProjectionNode projectionNode = new ProjectionNode(allRequiredFields);
    projectionNode.setAttr(PlanNode.INPUT_SCHEMA_ATTR, sourceSchema);
    projectionNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, projectedSchema);
    flowSpec.attachToLastLayer(projectionNode);

    if (where != null) {
      // Non-null filter conditions; apply the filter to all of our sources.
      String filterText = where.getText();
      PlanNode filterNode = new StrMatchFilterNode(filterText);
      flowSpec.attachToLastLayer(filterNode);
    }

    PlanContext outContext = planContext;
    if (planContext.isRoot()) {
      String selectTarget = planContext.getConf().get(CLIENT_SELECT_TARGET_KEY,
          DEFAULT_CLIENT_SELECT_TARGET);
      if (CONSOLE_SELECT_TARGET.equals(selectTarget)) {
        // SELECT statements that are root queries go to the console.
        flowSpec.attachToLastLayer(new ConsoleOutputNode(consoleFields));
      } else {
        // Client has specified that outputs of this root query go to a named memory buffer.
        flowSpec.attachToLastLayer(new MemoryOutputNode(selectTarget,
            distinctFields(consoleFields)));
      }
    } else {
      // If the initial projection contained both explicitly selected fields as
      // well as implicitly selected fields (e.g., for the WHERE clause), attach another
      // projection layer that extracts only the explicitly selected fields.

      // SELECT as a sub-query needs to create an output context with a
      // symbol table that contains the fields we expose through projection. 
      // We also need to set the output field names and output schema in our
      // returned context.
      outContext = new PlanContext(planContext);
      SymbolTable inTable = planContext.getSymbolTable();
      SymbolTable outTable = new HashSymbolTable(inTable);
      List<TypedField> outputFields = distinctFields(consoleFields);
      for (TypedField field : outputFields) {
        Symbol fieldSym = new Symbol(field.getName(), field.getType());
        outTable.addSymbol(fieldSym);
      }
      Schema outputSchema = createFieldSchema(outputFields);
      ProjectionNode cleanupProjection = new ProjectionNode(outputFields);
      projectionNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputSchema);
      flowSpec.attachToLastLayer(cleanupProjection);

      outContext.setSymbolTable(outTable);
      outContext.setSchema(outputSchema);
      outContext.setOutFields(outputFields);
    }

    return outContext;
  }
}

