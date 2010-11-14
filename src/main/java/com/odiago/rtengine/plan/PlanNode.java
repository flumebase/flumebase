// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract definition of a node in the query plan DAG.
 */
public abstract class PlanNode {
  /** All immediate predecessor nodes. */
  private List<PlanNode> mInputs;

  /** All immediate descendent nodes. */
  private List<PlanNode> mOutputs;

  private static int mNextId;

  /** node id within the DAG. */
  private int mId;

  public PlanNode() {
    mInputs = new ArrayList<PlanNode>();
    mOutputs = new ArrayList<PlanNode>();
    mId = mNextId++;
  }

  /** @return the list of child nodes. */
  public List<PlanNode> getChildren() {
    return mOutputs;
  }

  /**
   * Adds a new node to the DAG. Sets the specified node to be a child of
   * the current node, and sets this to be a parent of the specified node.
   */
  public void addChild(PlanNode child) {
    mOutputs.add(child);
    child.mInputs.add(this);
  }

  /**
   * Adds a new node to the DAG. Sets the specified node to be a parent of
   * the current node, and sets this to be a child of the specified node.
   */
  public void addParent(PlanNode parent) {
    mInputs.add(parent);
    parent.mInputs.add(this);
  }

  /** @return the element id within the DAG. */
  public int getId() {
    return mId;
  }

  /**
   * Format the node type and its parameters into the specified StringBuilder. */ 
  public abstract void formatParams(StringBuilder sb);

  @Override
  public String toString() {

    StringBuilder sbId = new StringBuilder();
    sbId.append(getId());
    String idStr = sbId.toString();

    StringBuilder sb = new StringBuilder();
    
    sb.append(idStr);
    sb.append(": ");
    formatParams(sb);
    sb.append("\n");
    
    formatGraphConnections(sb, "inputs", mInputs, idStr.length() + 2);
    formatGraphConnections(sb, "outputs", mOutputs, idStr.length() + 2);

    return sb.toString();
  }

  private void formatGraphConnections(StringBuilder sb, String connectionType,
      List<PlanNode> nodes, int indentLevel) {

    for (int i = 0; i < indentLevel; i++) {
      sb.append(" ");
    }
    sb.append(connectionType);
    sb.append(": ");
    boolean first = true;
    for (PlanNode node : nodes) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(node.getId());
      first = false;
    }
    sb.append("\n");
  }
}
