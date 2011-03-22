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
