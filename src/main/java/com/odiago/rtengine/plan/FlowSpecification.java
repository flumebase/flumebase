// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import org.apache.hadoop.conf.Configuration;

import com.odiago.rtengine.util.DAG;

/**
 * Specifies how FlowElements are deployed together
 * to form a complete flow. This is a DAG, which represents an abstract
 * plan to satisfy the query. This plan is not tied to particular physical
 * locations.
 */
public class FlowSpecification extends DAG<PlanNode> {
  private String mQuery;

  private Configuration mConf;

  public FlowSpecification(String query, Configuration conf) {
    mQuery = query;
    mConf = conf;
  }

  public FlowSpecification(Configuration conf) {
    mConf = conf;
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

  /**
   * @return the configuration options to use for this flow.
   */
  public Configuration getConf() {
    return mConf;
  }

  public void setConf(Configuration conf) {
    mConf = conf;
  }
}
