// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Generic node within a DAG.
 */
public class DAGNode<NODETYPE extends DAGNode> {
  /** All immediate predecessor nodes. */
  private List<NODETYPE> mInputs;

  /** All immediate descendent nodes. */
  private List<NODETYPE> mOutputs;

  /** node id within the graph. */
  private int mId;
  
  /**
   * The rank of a node is equal to 1 + max(rank(mInputs)).  Intuitively, the
   * root is rank 0; nodes that are reachable through the root are rank 1;
   * their children are rank 2, etc.  Nodes defined as rank 2 in the above
   * sentence are still rank-2 even if an additional edge exists from the root
   * to one of those nodes.
   */
  private int mRank;

  /**
   * Bit set to 'true' if the current traversal operator has seen
   * this node already.
   */
  private boolean mSeen;

  public DAGNode(int id) {
    mId = id;
    mInputs = new ArrayList<NODETYPE>();
    mOutputs = new ArrayList<NODETYPE>();
  }

  /** @return the list of child nodes. */
  public List<NODETYPE> getChildren() {
    return mOutputs;
  }

  /** @return the list of parent nodes. */
  public List<NODETYPE> getParents() {
    return mInputs;
  }

  /** @return true if this node has no parents. */
  public boolean isRoot() {
    return mInputs.size() == 0;
  }

  /** @return the rank of this node. */
  int getRank() {
    return mRank;
  }

  /** Set the rank of this node. */
  void setRank(int rank) {
    mRank = rank;
  }

  /**
   * Adds a new node to the DAG. Sets the specified node to be a child of
   * the current node, and sets this to be a parent of the specified node.
   */
  public void addChild(NODETYPE child) {
    mOutputs.add(child);
    child.mInputs.add(this);
  }

  /**
   * Adds a new node to the DAG. Sets the specified node to be a parent of
   * the current node, and sets this to be a child of the specified node.
   */
  public void addParent(NODETYPE parent) {
    mInputs.add(parent);
    parent.mOutputs.add(this);
  }

  /** @return the node's id within the graph. */
  public int getId() {
    return mId;
  }

  /** Set the node's id within the graph. */
  public void setId(int id) {
    mId = id;
  }

  /** Mark this node as seen by the current bfs/dfs/etc operator. */
  void markSeen() {
    mSeen = true;
  }

  /** Reset the mark in between operators. */
  void clearSeen() {
    mSeen = false;
  }

  /** @return true if the current traversal operator has seen this node already. */
  boolean isSeen() {
    return mSeen;
  }

  /**
   * Format the node type and its parameters into the specified StringBuilder.
   */ 
  protected void formatParams(StringBuilder sb) {
    sb.append("(");
    sb.append(getClass().getName());
    sb.append(")\n");
  }


  @Override
  public String toString() {

    StringBuilder sbId = new StringBuilder();
    sbId.append(getId());
    String idStr = sbId.toString();

    StringBuilder sb = new StringBuilder();
    
    sb.append(idStr);
    sb.append(": ");
    formatParams(sb);
    
    formatGraphConnections(sb, "inputs", getParents(), idStr.length() + 2);
    formatGraphConnections(sb, "outputs", getChildren(), idStr.length() + 2);

    return sb.toString();
  }

  protected void formatGraphConnections(StringBuilder sb, String connectionType,
      List<NODETYPE> nodes, int indentLevel) {

    for (int i = 0; i < indentLevel; i++) {
      sb.append(" ");
    }
    sb.append(connectionType);
    sb.append(": ");
    boolean first = true;
    for (NODETYPE node : nodes) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(node.getId());
      first = false;
    }
    sb.append("\n");
  }
    
}

