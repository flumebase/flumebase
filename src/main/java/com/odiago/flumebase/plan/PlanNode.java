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

package com.odiago.flumebase.plan;

import java.util.Map;
import java.util.TreeMap;

import com.odiago.flumebase.util.DAGNode;

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


  /**
   * Attribute referencing a Boolean indicating whether typical FlowElement
   * implementations of this node will require an eviction timer node. The
   * default value is false. See AggregateNode / BucketedAggregationElement for
   * an example use.
   */
  public static final String USES_TIMER_ATTR = "uses.timer";

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
    return getAttr(attrName, null);
  }

  /**
   * Get an attribute from the map. If the value retrieved for attrName is null,
   * returns defaultVal. Typically used by FlowSpecification.Operator
   * implementations while working over the graph.
   */
  public Object getAttr(String attrName, Object defaultVal) {
    Object ret = mAttributes.get(attrName);
    if (null == ret) {
      return defaultVal;
    }

    return ret;
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
