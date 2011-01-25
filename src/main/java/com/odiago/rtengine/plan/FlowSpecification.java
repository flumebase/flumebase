// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import com.odiago.rtengine.util.DAG;

/**
 * Specifies how FlowElements are deployed together
 * to form a complete flow. This is a DAG, which represents an abstract
 * plan to satisfy the query. This plan is not tied to particular physical
 * locations.
 */
public class FlowSpecification extends DAG<PlanNode> {
  private String mQuery;

  public FlowSpecification(String query) {
    mQuery = query;
  }

  public FlowSpecification() {
  }

  /**
   * @return the query associated with this flow.
   */
  public String getQuery() {
    return mQuery;
  }

  public void setQuery(String query) {
    mQuery = query;
  }
}
