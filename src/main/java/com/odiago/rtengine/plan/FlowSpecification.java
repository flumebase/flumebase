// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.odiago.rtengine.util.DAG;

/**
 * Specifies how FlowElements are deployed together
 * to form a complete flow. This is a DAG, which represents an abstract
 * plan to satisfy the query. This plan is not tied to particular physical
 * locations.
 */
public class FlowSpecification extends DAG<PlanNode> {
  public FlowSpecification() {
  }
}
