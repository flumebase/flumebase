// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.AssignedSymbol;
import com.odiago.rtengine.exec.EmptyEventWrapper;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.plan.FlowSpecification;
import com.odiago.rtengine.plan.HashJoinNode;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PlanNode;

/**
 * Represents two sources to a SELECT statement, married by a (windowed) JOIN clause.
 */
public class JoinedSource extends RecordSource {
  private static final Logger LOG = LoggerFactory.getLogger(
      JoinedSource.class.getName());

  private RecordSource mLeftSrc;
  private RecordSource mRightSrc;
  private Expr mJoinExpr;
  private Expr mWindowExpr;

  // All fields exposed by the joined streams.
  private SymbolTable mJoinedSymbols;

  // Symbol representing the key field for the left source.
  private Symbol mLeftKey;

  // Symbol representing the key field for the right source.
  private Symbol mRightKey;

  // Virtual name assigned to the output stream from this source.
  private String mJoinName;

  /**
   * Specifies a statement of the form "... &lt;leftSrc&gt; JOIN &lt;rightSrc&gt; ON ..."
   * @param leftSrc the "primary" source.
   * @param rightSrc the "secondary" source being joined in to the primary.
   * @param joinExpr the expression which must evaluate to true for the join to hold.
   * @param windowExpr an expression that resolves to a window specification, defining
   * the temporal boundaries of the join.
   */
  public JoinedSource(RecordSource leftSrc, RecordSource rightSrc, Expr joinExpr,
      Expr windowExpr) {
    mLeftSrc = leftSrc;
    mRightSrc = rightSrc;
    mJoinExpr = joinExpr;
    mWindowExpr = windowExpr;
  }

  public RecordSource getLeft() {
    return mLeftSrc;
  }

  public void setLeft(RecordSource left) {
    mLeftSrc = left;
  }

  public RecordSource getRight() {
    return mRightSrc;
  }

  public void setRight(RecordSource right) {
    mRightSrc = right;
  }

  public Expr getJoinExpr() {
    return mJoinExpr;
  }

  public void setJoinExpr(Expr joinExpr) {
    mJoinExpr = joinExpr;
  }

  public Expr getWindowExpr() {
    return mWindowExpr;
  }

  public void setWindowExpr(Expr windowExpr) {
    mWindowExpr = windowExpr;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getSourceNames() {
    List<String> out = new ArrayList<String>();
    out.addAll(mLeftSrc.getSourceNames());
    out.addAll(mRightSrc.getSourceNames());
    out.add(getSourceName());
    return out;
  }

  /** {@inheritDoc} */
  @Override
  public String getSourceName() {
    return mJoinName;
  }

  /**
   * Assign a name to the output of this join stmt, so that nested
   * joins can identify whether an input event is on the left or
   * right side.
   */
  public void setSourceName(String srcName) {
    mJoinName = srcName;
  }

  /** {@inheritDoc} */
  @Override
  public SymbolTable getFieldSymbols() {
    return mJoinedSymbols;
  }

  public void setJoinedSymbols(SymbolTable symTab) {
    mJoinedSymbols = symTab;
  }

  public void setLeftKey(Symbol leftKey) {
    mLeftKey = leftKey;
  }

  public Symbol getLeftKey() {
    return mLeftKey;
  }

  public void setRightKey(Symbol rightKey) {
    mRightKey = rightKey;
  }

  public Symbol getRightKey() {
    return mRightKey;
  }
  
  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("JOIN\n");
    pad(sb, depth + 1);
    sb.append("left:\n");
    mLeftSrc.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("right:\n");
    mRightSrc.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("ON:\n");
    mJoinExpr.format(sb, depth + 2);
    pad(sb, depth + 1);
    sb.append("OVER:\n");
    mWindowExpr.format(sb, depth + 2);
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    RecordSource leftSrc = getLeft();
    RecordSource rightSrc = getRight();

    // Create separate execution plans to gather data from our upstream sources.
    PlanContext leftContext = getSubPlan(leftSrc, planContext);
    PlanContext rightContext = getSubPlan(rightSrc, planContext);

    // Add our upstream source plans to our graph.
    FlowSpecification flowSpec = planContext.getFlowSpec();
    flowSpec.addNodesFromDAG(leftContext.getFlowSpec());
    flowSpec.addNodesFromDAG(rightContext.getFlowSpec());
    
    // Get the true field names that represent keys on the left and right
    // sides of the join.
    String leftName = leftSrc.getSourceName();
    AssignedSymbol leftSym = (AssignedSymbol) getLeftKey().resolveAliases();
    TypedField leftKey = new TypedField(leftSym.getAssignedName(), leftSym.getType());

    String rightName = rightSrc.getSourceName();
    AssignedSymbol rightSym = (AssignedSymbol) getRightKey().resolveAliases();
    TypedField rightKey = new TypedField(rightSym.getAssignedName(), rightSym.getType());

    WindowSpec window = null;
    try {
      // This should evaluate to itself, but make sure to resolve it anyway.
      assert mWindowExpr.isConstant();
      window = (WindowSpec) mWindowExpr.eval(new EmptyEventWrapper());
    } catch (IOException ioe) {
      // mWindowExpr should be constant, so this should be impossible.
      LOG.error("IOException calculating window expression: " + ioe);
      // Signal error by returning a null flow specification anyway.
      planContext.setFlowSpec(null);
      return planContext;
    }

    HashJoinNode joinNode = new HashJoinNode(leftName, rightName, leftKey, rightKey,
        window, getSourceName(), leftContext.getOutFields(), rightContext.getOutFields(),
        planContext.getConf());

    // Set this node to expect multiple input schemas.
    List<Schema> inputSchemas = new ArrayList<Schema>();
    inputSchemas.add(leftContext.getSchema());
    inputSchemas.add(rightContext.getSchema());
    joinNode.setAttr(PlanNode.MULTI_INPUT_SCHEMA_ATTR, inputSchemas);

    flowSpec.attachToLastLayer(joinNode);

    // Create an output context defining our fields, etc.
    PlanContext outContext = new PlanContext(planContext);

    SymbolTable outTable = SymbolTable.mergeSymbols(leftContext.getSymbolTable(),
        rightContext.getSymbolTable(), planContext.getSymbolTable());
    outContext.setSymbolTable(outTable);

    List<TypedField> outputFields = new ArrayList<TypedField>();
    outputFields.addAll(leftContext.getOutFields());
    outputFields.addAll(rightContext.getOutFields());
    outputFields = distinctFields(outputFields);
    outContext.setOutFields(outputFields);

    Schema outSchema = createFieldSchema(outputFields);
    outContext.setSchema(outSchema);
    joinNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outSchema);

    return outContext;
  }
}
