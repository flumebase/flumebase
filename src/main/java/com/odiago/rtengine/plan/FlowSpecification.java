// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Specifies how FlowElements are deployed together
 * to form a complete flow. This is a DAG, which represents an abstract
 * plan to satisfy the query. This plan is not tied to particular physical
 * locations.
 */
public class FlowSpecification {
  
  private List<PlanNode> roots;

  public FlowSpecification() {
    roots = new ArrayList<PlanNode>();
  }

  /**
   * An operator which should be applied to all elements of the FlowSpecification.
   */
  public abstract static class Operator {
    public abstract void process(PlanNode node);
  }

  /**
   * Adds the specified node as a root of the DAG.
   */
  public void addRoot(PlanNode node) {
    roots.add(node);
  }

  /**
   * Given a complete flow specification for a sub-flow 'other', add
   * its nodes to our flow specification.
   */
  public void addNodesFromPlan(FlowSpecification other) {
    roots.addAll(other.roots);
  }

  /**
   * Perform a breadth-first traversal over the flow specification,
   * applying the specified operator to each node.
   */
  public void bfs(Operator op) {
    // Work queue for BFS.
    List<PlanNode> work = new LinkedList<PlanNode>();
    work.addAll(roots);
    while (!work.isEmpty()) {
      PlanNode curNode = work.remove(0);
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
    List<PlanNode> work = new LinkedList<PlanNode>();
    work.addAll(roots);
    while (!work.isEmpty()) {
      PlanNode curNode = work.remove(work.size() - 1);
      op.process(curNode);
      work.addAll(curNode.getChildren());
    }
  }

  /**
   * @return a list of all nodes with no outputs.
   */
  public List<PlanNode> getLastLayer() {
    
    final List<PlanNode> lastLayer = new ArrayList<PlanNode>();

    // Execute an operator over all nodes; gather a list of all
    // nodes which do not have children.
    bfs(new Operator() {
      @Override
      public void process(PlanNode node) {
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
  public void attachToLastLayer(PlanNode node) {
    List<PlanNode> lastLayer = getLastLayer();
    for (PlanNode parent : lastLayer) {
      parent.addChild(node);
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();

    // Use a BFS operator to stringify all nodes in the DAG.
    bfs(new Operator() {
      @Override
      public void process(PlanNode node) {
        sb.append(node.toString());
      }
    });

    return sb.toString();
  }
}
