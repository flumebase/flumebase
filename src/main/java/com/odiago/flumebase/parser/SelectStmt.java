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

package com.odiago.flumebase.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.plan.AggregateNode;
import com.odiago.flumebase.plan.OutputNode;
import com.odiago.flumebase.plan.EvaluateExprsNode;
import com.odiago.flumebase.plan.FilterNode;
import com.odiago.flumebase.plan.FlowSpecification;
import com.odiago.flumebase.plan.MemoryOutputNode;
import com.odiago.flumebase.plan.PlanContext;
import com.odiago.flumebase.plan.PlanNode;
import com.odiago.flumebase.plan.ProjectionNode;

import com.odiago.flumebase.util.StringUtils;

/**
 * SELECT statement.
 */
public class SelectStmt extends RecordSource {

  private static final Logger LOG = LoggerFactory.getLogger(SelectStmt.class.getName());

  /**
   * Configuration key that specifies how we should deliver output records of a
   * top-level rtsql statement to the client. If this is set to "$console," we
   * print to the screen. Other strings cause us to allocate a list buffer that
   * can be retrieved later by the client.
   */
  public static final String CLIENT_SELECT_TARGET_KEY = "flumebase.client.select.target";

  /** Special value for flumebase.client.select.target that prints to stdout. */
  public static final String CONSOLE_SELECT_TARGET = "$console";

  /** The default for flumebase.client.select.target is to use the console. */
  public static final String DEFAULT_CLIENT_SELECT_TARGET = CONSOLE_SELECT_TARGET;

  /** Set of fields or other expressions to select */
  private List<AliasedExpr> mSelectExprs;

  // Source stream for the FROM clause. must be a LiteralSource or a SelectStmt.
  // (That fact is proven by a TypeChecker visitor.)
  private SQLStatement mSource;

  // Expression that must evaluate to true in the WHERE clause to accept records.
  // (may be null)
  private Expr mWhereExpr;

  // GROUP BY clause (may be null).
  private GroupBy mGroupBy;

  // OVER clause (may be null); expr specifies the window we aggregate on.
  private Expr mAggregateOver;

  // Expressions in the SELECT statement that are produced by aggregate functions.
  // (provided by IdentifyAggregates visitor pass).
  private List<AliasedExpr> mAggregateExprs;

  // Expression that must evaluate to true in the HAVING clause to accept records.
  // (may be null)
  private Expr mHaving;

  // List of window definitions; bindings from identifiers to WindowSpecs
  // in the scope of this SELECT statement.
  private List<WindowDef> mWindowDefs;

  /** User-specified alias for the ephemeral stream containing the results of this
   * SELECT statement inside another SELECT statement.
   */
  private String mAlias;

  /**
   * Name associated with an output logical node emitting the select's results
   * into Flume. (May be null.)
   */
  private String mOutputName;

  /**
   * All symbols representing fields available as output of this select stmt.
   */
  private SymbolTable mFieldSymbols;

  public SelectStmt(List<AliasedExpr> selExprs, SQLStatement source, Expr where,
      GroupBy groupBy, Expr aggregateOver, Expr having, List<WindowDef> windowDefs) {
    mSelectExprs = selExprs;
    mSource = source;
    mWhereExpr = where;
    mGroupBy = groupBy;
    mAggregateOver = aggregateOver;
    mHaving = having;
    mWindowDefs = windowDefs;
  }

  public List<AliasedExpr> getSelectExprs() {
    return mSelectExprs;
  }

  public SQLStatement getSource() {
    return mSource;
  }

  public void setSource(SQLStatement src) {
    mSource = src;
  }

  public Expr getWhereConditions() {
    return mWhereExpr;
  }

  public void setWhereConditions(Expr where) {
    mWhereExpr = where;
  }

  public Expr getHaving() {
    return mHaving;
  }

  public void setHaving(Expr having) {
    mHaving = having;
  }

  public GroupBy getGroupBy() {
    return mGroupBy;
  }

  public void setGroupBy(GroupBy groupBy) {
    mGroupBy = groupBy;
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

  public String getOutputName() {
    return mOutputName;
  }

  public void setOutputName(String outputName) {
    mOutputName = outputName;
  }

  public List<AliasedExpr> getAggregateExprs() {
    return mAggregateExprs;
  }

  public Expr getWindowOver() {
    return mAggregateOver;
  }

  public void setWindowOver(Expr windowOver) {
    mAggregateOver = windowOver;
  }

  public void setAggregateExprs(List<AliasedExpr> aggregateExprs) {
    mAggregateExprs = aggregateExprs;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getSourceNames() {
    if (null != mAlias) {
      return Collections.singletonList(mAlias);
    } else {
      return Collections.emptyList();
    }
  }

  /** {@inheritDoc} */
  @Override
  public String getSourceName() {
    return mAlias;
  }

  /**
   * After calculating a SymbolTable containing all the fields
   * of this select statement, attach it to the statement for future use.
   */
  public void setFieldSymbols(SymbolTable fieldSymbols) {
    mFieldSymbols = fieldSymbols;
  }

  /** {@inheritDoc} */
  @Override
  public SymbolTable getFieldSymbols() {
    return mFieldSymbols;
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

    if (null != mGroupBy) {
      mGroupBy.format(sb, depth + 1);
    }

    if (null != mAggregateOver) {
      pad(sb, depth + 1);
      sb.append("OVER\n");
      mAggregateOver.format(sb, depth + 2);
    }

    if (null != mHaving) {
      pad(sb, depth + 1);
      sb.append("HAVING\n");
      mHaving.format(sb, depth + 2);
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

    if (null != mOutputName) {
      pad(sb, depth + 1);
      sb.append("OUTPUT AS: outputName=");
      sb.append(mOutputName);
      sb.append("\n");
    }
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    SQLStatement source = getSource();
    Expr where = getWhereConditions();

    // Create an execution plan for the source(s) of this SELECT stream.
    PlanContext sourceOutCtxt = getSubPlan(source, planContext);
    SymbolTable srcOutSymbolTable = sourceOutCtxt.getSymbolTable();

    // Now incorporate that entire plan into our plan.
    FlowSpecification flowSpec = planContext.getFlowSpec();
    flowSpec.addNodesFromDAG(sourceOutCtxt.getFlowSpec());

    // List of all fields required as output from the source node.
    List<TypedField> allRequiredFields = new ArrayList<TypedField>();

    // All fields carried forward by the aggregation layer from the source layer.
    List<TypedField> groupByPropagateFields = new ArrayList<TypedField>();

    // Another list holds all the fields which the EvaluateExprsNode will need to
    // propagate from the initial source layer forward.
    List<TypedField> exprPropagateFields = new ArrayList<TypedField>();

    // List of all fields with their input names that should be read by the ProjectionNode.
    // This is exprPropagateFields + fields emitted by the expr layer.
    List<TypedField> projectionInputs = new ArrayList<TypedField>();

    // List of all fields returned from the ProjectionNode; this layer
    // uses the translated names from the "x AS y" clauses.
    List<TypedField> projectionOutputs = new ArrayList<TypedField>();

    // Create a list containing the (ordered) set of fields we want emitted to the console.
    List<TypedField> consoleFields = new ArrayList<TypedField>();

    // Populate the field lists defined above
    calculateRequiredFields(srcOutSymbolTable, sourceOutCtxt.getOutFields(),
        allRequiredFields, groupByPropagateFields, exprPropagateFields,
        projectionInputs, projectionOutputs, consoleFields);

    if (where != null) {
      // Non-null filter conditions; apply the filter to all of our sources.
      PlanNode filterNode = new FilterNode(where);
      flowSpec.attachToLastLayer(filterNode);
    }

    // Add an aggregation layer, if required.
    addAggregationToPlan(srcOutSymbolTable, flowSpec, groupByPropagateFields);

    // Evaluate calculated-expression fields.
    addExpressionsToPlan(flowSpec, exprPropagateFields, projectionInputs);

    // Create the projected schema based on the symbol table returned by our source. 
    Schema projectedSchema = createFieldSchema(distinctFields(projectionOutputs));
    ProjectionNode projectionNode = new ProjectionNode(projectionInputs, projectionOutputs);
    projectionNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, projectedSchema);
    flowSpec.attachToLastLayer(projectionNode);

    if (mHaving != null) {
      // Non-null HAVING conditions; apply another filter to our output.
      PlanNode havingNode = new FilterNode(mHaving);
      flowSpec.attachToLastLayer(havingNode);
    }

    return createReturnedContext(planContext, consoleFields);
  }

  /**
   * Analyze the expressions in the SELECT field projection list, the WHERE
   * clause, etc. and determine which fields of the underlying stream
   * need to be pulled out into the intermediate and result records.
   * @param fieldSymbols the SymbolTable returned by the source which defines
   * the types of all the fields of the source stream(s).
   * @param srcOutFields the list of all fields available as output from the
   * source.
   * @param allRequiredFields (output) - all fields required as output from
   * the source (e.g., because they are consoleFields, or used in other
   * expressions in the WHERE clause).
   * @param groupByPropagateFields (output) - the fields the aggregate eval
   * layer carries forward and passes through to its output.
   * @param exprPropagateFields (output) - the fields which the expression
   * evaluation layer carries forward and passes through to its output.
   * @param projectionInputs (output) - the fields that should be read by the
   * ProjectionNode and carried through to its output.
   * @param projectionOutputs (output) - the same set of fields as
   * projectionInputs, after being transformed by the projection layer.
   * @param consoleFields (output) - the list of fields that should be
   * presented to the console (or other sink for this SELECT statement).
   */
  private void calculateRequiredFields(SymbolTable fieldSymbols,
      List<TypedField> srcOutFields,
      List<TypedField> allRequiredFields,
      List<TypedField> groupByPropagateFields,
      List<TypedField> exprPropagateFields,
      List<TypedField> projectionInputs,
      List<TypedField> projectionOutputs,
      List<TypedField> consoleFields) {

    // Start with all the fields the user explicitly selected.
    List<AliasedExpr> exprList = getSelectExprs();
    for (AliasedExpr aliasExpr : exprList) {
      Expr e = aliasExpr.getExpr();
      if (e instanceof AllFieldsExpr) {
        // Use all field names listed as outputs from the source's output context.
        for (TypedField outField : srcOutFields) {
          allRequiredFields.add(outField);
          groupByPropagateFields.add(outField);
          exprPropagateFields.add(outField);
          projectionInputs.add(outField);
          projectionOutputs.add(outField);
          consoleFields.add(outField);
        }
      } else {
        // Get the type within the expression, and add the appropriate labels.
        // These have been already assigned by a visitor pass.

        Type t = e.getType(fieldSymbols);
        TypedField projectionField = new TypedField(
          aliasExpr.getUserAlias(), t,
          aliasExpr.getAvroLabel(), aliasExpr.getDisplayLabel());

        // Make sure our dependencies are pulled out of the source layer.
        List<TypedField> fieldsForExpr = e.getRequiredFields(fieldSymbols);

        if (e instanceof IdentifierExpr) {
          // The aggregation and expression evaluation nodes need to
          // carry this field forward into the output.
          // Make sure to use the aliased name as the output of the
          // projection/expr-propagate layers, but use the original name as
          // the output of the source layer (projection input list).
          groupByPropagateFields.add(projectionField);
          exprPropagateFields.add(projectionField);
        } else if (mAggregateExprs.contains(aliasExpr)) {
          // Calculated in the aggregation layer.
          // Carry result forward through expr eval.
          exprPropagateFields.add(projectionField);
          // Pull dependencies from source layer.
          allRequiredFields.addAll(fieldsForExpr);
        } else {
          // This is calculated in the expression evaluation layer.
          allRequiredFields.addAll(fieldsForExpr);
          groupByPropagateFields.addAll(fieldsForExpr); // Propagate dependencies forward..
        }

        // Regardless of which calculation stage generated the field, this
        // result is carried through to the end of the query.
        projectionInputs.add(projectionField);
        projectionOutputs.add(projectionField);
        consoleFields.add(projectionField);
      }
    }

    Expr where = getWhereConditions();
    if (null != where) {
      // Add to this all the fields required by the where clause.
      List<TypedField> whereReqs = where.getRequiredFields(fieldSymbols);
      allRequiredFields.addAll(whereReqs);
    }

    if (null != mGroupBy) {
      // Add to this all the fields required for grouping in the GROUP BY clause.
      allRequiredFields.addAll(mGroupBy.getFieldTypes());
    }

    allRequiredFields = distinctFields(allRequiredFields);
    exprPropagateFields = distinctFields(exprPropagateFields);
    // Important: the ProjectionElement requires these to have the same arity
    // and order.
    projectionInputs = distinctFields(projectionInputs);
    projectionOutputs = distinctFields(projectionOutputs);
    assert projectionInputs.size() == projectionOutputs.size();
  }

  private void addAggregationToPlan(SymbolTable fieldSymbols, FlowSpecification flowSpec,
      List<TypedField> groupByPropagateFields) {

    if (null != mAggregateExprs && mAggregateExprs.size() > 0) {
      // Non-null aggregate expression list; add an aggregation step to our plan.

      List<TypedField> aggregateOverFields = Collections.emptyList();
      if (null != mGroupBy) {
        aggregateOverFields = mGroupBy.getFieldTypes();
      }

      LOG.debug("Aggregate exprs: " + StringUtils.listToStr(mAggregateExprs));
      assert flowSpec.getConf() != null;
      PlanNode aggregateNode = new AggregateNode(aggregateOverFields,
          mAggregateOver, mAggregateExprs, groupByPropagateFields, flowSpec.getConf());
      flowSpec.attachToLastLayer(aggregateNode);

      // Output schema for this layer contains everything we need to forward
      // from our upstream layers...
      List<TypedField> aggOutputFields = new ArrayList<TypedField>();
      aggOutputFields.addAll(groupByPropagateFields);
      // As well as the names of everything we calculate in this layer.
      for (AliasedExpr aliasExpr : mAggregateExprs) {
        Expr e = aliasExpr.getExpr();
        Type t = e.getType(fieldSymbols);
        TypedField aggregateField = new TypedField(
          aliasExpr.getUserAlias(), t,
          aliasExpr.getAvroLabel(), aliasExpr.getDisplayLabel());
        aggOutputFields.add(aggregateField);
      }
      Schema aggregateOutSchema = createFieldSchema(aggOutputFields);
      aggregateNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, aggregateOutSchema);
    }
  }

  /**
   * If we output columns which are based on computed (non-aggregate)
   * expressions, add an expression computation node to the flow
   * specification.
   */
  private void addExpressionsToPlan(FlowSpecification flowSpec,
      List<TypedField> exprPropagateFields, List<TypedField> projectionInputs) {
    List<AliasedExpr> calculatedExprs = new ArrayList<AliasedExpr>();
    for (AliasedExpr expr : getSelectExprs()) {
      Expr subExpr = expr.getExpr();
      if (!(subExpr instanceof AllFieldsExpr) && !(subExpr instanceof IdentifierExpr)
          && !(mAggregateExprs.contains(expr))) {
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
  }

  /**
   * Create the output PlanContext that should be returned by createExecPlan().
   */
  private PlanContext createReturnedContext(PlanContext planContext,
      List<TypedField> outputFields) {
    PlanContext outContext = planContext;
    FlowSpecification flowSpec = planContext.getFlowSpec();
    if (planContext.isRoot()) {
      String selectTarget = planContext.getConf().get(CLIENT_SELECT_TARGET_KEY,
          DEFAULT_CLIENT_SELECT_TARGET);
      if (CONSOLE_SELECT_TARGET.equals(selectTarget)) {
        // SELECT statements that are root queries go to the output node.

        // This output node may emit Avro records to a Flume node. These records
        // should use more user-friendly names for the fields than the anonymized
        // field names we use internally. Create a final schema for the output
        // plan node.
        String outputName = getOutputName();
        List<TypedField> outSchemaFields = new ArrayList<TypedField>();
        List<TypedField> distinctOutFields = distinctFields(outputFields);
        for (TypedField outField : distinctOutFields) {
          String safeName = avroSafeName(outField.getDisplayName());
          outSchemaFields.add(new TypedField(safeName, outField.getType()));
        }
        Schema finalSchema = createFieldSchema(outSchemaFields, outputName);
        OutputNode outputNode = new OutputNode(outputFields, outSchemaFields, outputName);
        outputNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, finalSchema);
        flowSpec.attachToLastLayer(outputNode);
      } else {
        // Client has specified that outputs of this root query go to a named memory buffer.
        flowSpec.attachToLastLayer(new MemoryOutputNode(selectTarget,
            distinctFields(outputFields)));
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
      outputFields = distinctFields(outputFields);
      outTable.addAll(mFieldSymbols);
      Schema outputSchema = createFieldSchema(outputFields);
      ProjectionNode cleanupProjection = new ProjectionNode(outputFields, outputFields);
      cleanupProjection.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputSchema);
      flowSpec.attachToLastLayer(cleanupProjection);

      outContext.setSymbolTable(outTable);
      outContext.setSchema(outputSchema);
      outContext.setOutFields(outputFields);
    }

    return outContext;
  }
}

