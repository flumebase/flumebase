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

import org.apache.hadoop.conf.Configuration;

import com.odiago.flumebase.util.DAG;

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
