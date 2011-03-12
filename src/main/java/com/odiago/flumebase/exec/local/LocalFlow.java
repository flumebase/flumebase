// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import org.apache.hadoop.conf.Configuration;

import com.odiago.flumebase.exec.FlowId;
import com.odiago.flumebase.util.DAG;

/**
 * A DAG of FlowElements to be executed in the local context.
 */
public class LocalFlow extends DAG<FlowElementNode> {
  private FlowId mFlowId;

  private boolean mRequiresFlume; 
  private String mQuery;
  private Configuration mConf;
  private boolean mIsDeployed;

  public LocalFlow(FlowId id) {
    mFlowId = id;
    mRequiresFlume = false;
    mQuery = null;
    mConf = null;
    mIsDeployed = false;
  }

  public FlowId getId() {
    return mFlowId;
  }

  /**
   * @return true if deployment of the flow is complete.
   */
  public boolean isDeployed() {
    return mIsDeployed;
  }

  void setDeployed(boolean deployed) {
    mIsDeployed = deployed;
  }

  /**
   * @return true if Flume is required locally to execute this flow.
   */
  public boolean requiresFlume() {
    return mRequiresFlume;
  }

  /**
   * Called by the LocalFlowBuilder to specify whether this flow requires
   * local Flume elements to be started first.
   */
  void setFlumeRequired(boolean required) {
    mRequiresFlume = required;
  }

  /** Set the query string that this flow represents. */
  void setQuery(String query) {
    mQuery = query;
  }

  public String getQuery() {
    return mQuery;
  }

  public void setConf(Configuration conf) {
    mConf = conf;
  }

  /** @return the configuration governing this flow's behavior. */
  public Configuration getConf() {
    return mConf;
  }

  @Override
  public String toString() {
    return "flow(id=" + mFlowId + ")\n" + super.toString();
  }
}
