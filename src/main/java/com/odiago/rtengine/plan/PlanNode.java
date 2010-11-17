// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.Map;
import java.util.TreeMap;

import com.odiago.rtengine.util.DAGNode;

/**
 * Abstract definition of a node in the query plan DAG.
 */
public class PlanNode extends DAGNode<PlanNode> {

  /** Free-form attribute map which can be used by operators when working on
   * transforming the graph, etc.
   */
  private Map<String, Object> mAttributes;

  private static int mNextId;

  public PlanNode() {
    super(mNextId++);
    mAttributes = new TreeMap<String, Object>();
  }

  /**
   * Set an attribute in the map. Typically used by FlowSpecification.Operator
   * implementations while working over the graph.
   */
  public void setAttr(String attrName, Object attrVal) {
    mAttributes.put(attrName, attrVal);
  }

  /**
   * Get an attribute from the map. Typically used by FlowSpecification.Operator
   * implementations while working over the graph.
   */
  public Object getAttr(String attrName) {
    return mAttributes.get(attrName);
  }
}
