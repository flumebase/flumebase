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

import com.odiago.rtengine.lang.Type;

import com.odiago.rtengine.plan.ConsoleOutputNode;
import com.odiago.rtengine.plan.EvaluateExprsNode;
import com.odiago.rtengine.plan.FilterNode;
import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.MemoryOutputNode;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PlanNode;
import com.odiago.rtengine.plan.ProjectionNode;

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

  // Expression that must evaluate to true in the WHERE clause to accept records.
  private Expr mWhereExpr;

  // List of window definitions; bindings from identifiers to WindowSpecs
  // in the scope of this SELECT statement.
  private List<WindowDef> mWindowDefs;

  /** User-specified alias for the ephemeral stream containing the results of this
   * SELECT statement inside another SELECT statement.
   */
  private String mAlias;

  /**
   * All symbols representing fields available as output of this select stmt.
   */
  private SymbolTable mFieldSymbols;

  public SelectStmt(List<AliasedExpr> selExprs, SQLStatement source, Expr where,
      List<WindowDef> windowDefs) {
    mSelectExprs = selExprs;
    mSource = source;
    mWhereExpr = where;
    mWindowDefs = windowDefs;
  }

  public List<AliasedExpr> getSelectExprs() {
    return mSelectExprs;
  }

  public SQLStatement getSource() {
    return mSource;
  }

  public Expr getWhereConditions() {
    return mWhereExpr;
  }

  public List<WindowDef> getWindowDefs() {
    return mWindowDefs;
  }

  public String getAlias() {
    return mAlias;
  }

  public void setAlias(String alias) {
    mAlias = alias;
  }

  /**
   * After calculating a SymbolTable containing all the fields
   * of this select statement, attach it to the statement for future use.
   */
  public void setFieldSymbols(SymbolTable fieldSymbols) {
    mFieldSymbols = fieldSymbols;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("SELECT");
    sb.append("\n");
    pad(sb, depth + 1);
    sb.append("expressions:\n");
    for (AliasedExpr ae : mSelectExprs) {
      ae.format(sb, depth + 2);
    }
    pad(sb, depth + 1);
    sb.append("FROM:\n");
    mSource.format(sb, depth + 2);
    if (null != mWhereExpr) {
      pad(sb, depth + 1);
      sb.append("WHERE\n");
      mWhereExpr.format(sb, depth + 2);
    }

    if (mWindowDefs.size() > 0) {
      pad(sb, depth + 1);
      sb.append("Windows:\n");
      for (WindowDef def : mWindowDefs) {
        def.format(sb, depth + 2);
      }
    }

    if (null != mAlias) {
      pad(sb, depth + 1);
      sb.append("AS: alias=");
      sb.append(mAlias);
      sb.append("\n");
    }
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    SQLStatement source = getSource();
    Expr where = getWhereConditions();

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

    // List of all fields required as output from the source node.
    List<TypedField> allRequiredFields = new ArrayList<TypedField>();

    // List of all fields with their input names that should be read by the ProjectionNode.
    List<TypedField> projectionInputs = new ArrayList<TypedField>();

    // List of all fields returned from the ProjectionNode; this layer
    // uses the translated names from the "x AS y" clauses.
    List<TypedField> projectionOutputs = new ArrayList<TypedField>();

    // Create a list containing the (ordered) set of fields we want emitted to the console.
    List<TypedField> consoleFields = new ArrayList<TypedField>();

    // Another list holds all the fields which the EvaluateExprsNode will need to
    // propagate from the initial source layer forward.
    List<TypedField> exprPropagateFields = new ArrayList<TypedField>();

    // Start with all the fields the user explicitly selected.
    List<AliasedExpr> exprList = getSelectExprs();
    for (AliasedExpr aliasExpr : exprList) {
      Expr e = aliasExpr.getExpr();
      if (e instanceof AllFieldsExpr) {
        // Use all field names listed as outputs from the source's output context.
        for (TypedField outField : sourceOutCtxt.getOutFields()) {
          consoleFields.add(outField);
          allRequiredFields.add(outField);
          projectionInputs.add(outField);
          projectionOutputs.add(outField);
          exprPropagateFields.add(outField);
        }
      } else {
        // Get the type within the expression, and add the appropriate labels.
        // These have been already assigned by a visitor pass.

        Type t = e.getType(srcOutSymbolTable);
        TypedField projectionField = new TypedField(
          aliasExpr.getUserAlias(), t,
          aliasExpr.getAvroLabel(), aliasExpr.getDisplayLabel());

        projectionInputs.add(projectionField);
        projectionOutputs.add(projectionField);
        consoleFields.add(projectionField);

        // Make sure our dependencies are pulled out of the source layer.
        List<TypedField> fieldsForExpr = e.getRequiredFields(srcOutSymbolTable);
        allRequiredFields.addAll(fieldsForExpr);

        if (e instanceof IdentifierExpr) {
          // The expr evaluation node needs to carry this field forward
          // into its output. Make sure to use the aliased name as the output
          // of the projection/expr-propagate layers, but use the original
          // name as the output of the source layer (projection input list).
          exprPropagateFields.add(projectionField);
        }
      }
    }

    if (null != where) {
      // Add to this all the fields required by the where clause.
      List<TypedField> whereReqs = where.getRequiredFields(srcOutSymbolTable);
      allRequiredFields.addAll(whereReqs);
    }

    allRequiredFields = distinctFields(allRequiredFields);
    exprPropagateFields = distinctFields(exprPropagateFields);
    // Important: the ProjectionElement requires these to have the same arity
    // and order.
    projectionInputs = distinctFields(projectionInputs);
    projectionOutputs = distinctFields(projectionOutputs);

    if (where != null) {
      // Non-null filter conditions; apply the filter to all of our sources.
      PlanNode filterNode = new FilterNode(where);
      flowSpec.attachToLastLayer(filterNode);
    }

    // Evaluate calculated-expression fields.
    List<AliasedExpr> calculatedExprs = new ArrayList<AliasedExpr>();
    for (AliasedExpr expr : getSelectExprs()) {
      Expr subExpr = expr.getExpr();
      if (!(subExpr instanceof AllFieldsExpr) && !(subExpr instanceof IdentifierExpr)) {
        calculatedExprs.add(expr);
      }
    }

    if (calculatedExprs.size() > 0) {
      PlanNode exprNode = new EvaluateExprsNode(calculatedExprs, exprPropagateFields);
      // TODO(aaron): assert that calculatedExprs UNION exprPropagateFields gives
      // us the projectionInputs list.
      Schema exprOutSchema = createFieldSchema(projectionInputs);
      exprNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, exprOutSchema);
      flowSpec.attachToLastLayer(exprNode);
    }

    // Create the projected schema based on the symbol table returned by our source. 
    Schema projectedSchema = createFieldSchema(projectionOutputs);
    ProjectionNode projectionNode = new ProjectionNode(projectionInputs, projectionOutputs);
    projectionNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, projectedSchema);
    flowSpec.attachToLastLayer(projectionNode);


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
      outTable.addAll(mFieldSymbols);
      Schema outputSchema = createFieldSchema(outputFields);
      ProjectionNode cleanupProjection = new ProjectionNode(outputFields, outputFields);
      projectionNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputSchema);
      flowSpec.attachToLastLayer(cleanupProjection);

      outContext.setSymbolTable(outTable);
      outContext.setSchema(outputSchema);
      outContext.setOutFields(outputFields);
    }

    return outContext;
  }
}

