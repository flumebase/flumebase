// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Generic DAG of nodes.
 */
public class DAG<NODETYPE extends DAGNode<NODETYPE>> {
  
  private List<NODETYPE> roots;

  public DAG() {
    roots = new ArrayList<NODETYPE>();
  }

  /**
   * An operator which should be applied to all elements of the DAG.
   */
  public abstract static class Operator<NODE extends DAGNode<NODE>> {
    public abstract void process(NODE node);
  }

  /**
   * Adds the specified node as a root of the DAG.
   */
  public void addRoot(NODETYPE node) {
    roots.add(node);
  }

  /**
   * Given a complete flow specification for a sub-flow 'other', add
   * its nodes to our flow specification.
   */
  public void addNodesFromDAG(DAG<NODETYPE> other) {
    roots.addAll(other.roots);
  }

  /**
   * Perform a breadth-first traversal over the flow specification,
   * applying the specified operator to each node.
   */
  public void bfs(Operator op) {
    // Work queue for BFS.
    List<NODETYPE> work = new LinkedList<NODETYPE>();
    work.addAll(roots);
    while (!work.isEmpty()) {
      NODETYPE curNode = work.remove(0);
      op.process(curNode);
      work.addAll(curNode.getChildren());
    }
  }

  /**
   * Perform a depth-first traversal over the flow specification,
   * applying the specified operator to each node.
   */
  public void dfs(Operator op) {
    // Work stack for DFS.
    List<NODETYPE> work = new LinkedList<NODETYPE>();
    work.addAll(roots);
    while (!work.isEmpty()) {
      NODETYPE curNode = work.remove(work.size() - 1);
      op.process(curNode);
      work.addAll(curNode.getChildren());
    }
  }

  /**
   * Perform a breadth-first search "in reverse", starting at the final nodes
   * in the DAG and working backward toward the roots, applying the specified
   * operator to each node.
   */
  public void reverseBfs(Operator op) {
    List<NODETYPE> work = new LinkedList<NODETYPE>();
    work.addAll(getLastLayer());
    while (!work.isEmpty()) {
      NODETYPE curNode = work.remove(0);
      op.process(curNode);
      work.addAll(curNode.getParents());
    }
  }

  /**
   * @return a list of all nodes with no outputs.
   */
  public List<NODETYPE> getLastLayer() {
    
    final List<NODETYPE> lastLayer = new ArrayList<NODETYPE>();

    // Execute an operator over all nodes; gather a list of all
    // nodes which do not have children.
    bfs(new Operator<NODETYPE>() {
      @Override
      public void process(NODETYPE node) {
        if (node.getChildren().size() == 0) {
          lastLayer.add(node);
        }
      }
    });

    return lastLayer;
  }

  /**
   * Sets 'node' as a child of every node in the current "bottom" layer
   * of the DAG.
   */
  public void attachToLastLayer(NODETYPE node) {
    List<NODETYPE> lastLayer = getLastLayer();
    for (NODETYPE parent : lastLayer) {
      parent.addChild(node);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    // Use a BFS operator to stringify all nodes in the DAG.
    bfs(new Operator<NODETYPE>() {
      @Override
      public void process(NODETYPE node) {
        sb.append(node.toString());
      }
    });

    return sb.toString();
  }
}
