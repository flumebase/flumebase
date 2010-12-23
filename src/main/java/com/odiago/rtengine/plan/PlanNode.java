// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.Map;
import java.util.TreeMap;

import com.odiago.rtengine.util.DAGNode;

/**
 * Abstract definition of a node in the query plan DAG.
 */
public class PlanNode extends DAGNode<PlanNode> {

  /**
   * Attribute referencing a Schema describing the set of input fields
   * to this node in the plan. 
   */
  public static final String INPUT_SCHEMA_ATTR = "input.field.schema";

  /**
   * Attribute referencing a Schema describing the set of output fields
   * from this node in the plan. 
   */
  public static final String OUTPUT_SCHEMA_ATTR = "output.field.schema";

  /**
   * Attribute referencing a List&lt;Schema&gt; describing sets of input
   * fields that may arrive from one of the multiple sources to this node.
   * If this is set, INUPT_SCHEMA_ATTR is expected to be unset.
   */
  public static final String MULTI_INPUT_SCHEMA_ATTR = "input.field.multi.schemas";

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

  /** Format all free-form attributes of the node into the specified StringBuilder. */
  public void formatAttributes(StringBuilder sb) {
    for (Map.Entry<String, Object> attr : mAttributes.entrySet()) {
      sb.append("  ");
      sb.append(attr.getKey());
      sb.append(" : ");
      sb.append(attr.getValue());
      sb.append("\n");
    }
  }

  protected void formatParams(StringBuilder sb) {
    sb.append("(");
    sb.append(getClass().getName());
    sb.append(")\n");
    formatAttributes(sb);
  }
}
