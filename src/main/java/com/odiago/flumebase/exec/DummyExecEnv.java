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

package com.odiago.flumebase.exec;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.plan.FlowSpecification;

import com.odiago.flumebase.server.SessionId;

/**
 * Execution engine that logs an error for every operation. This execution engine
 * is used when the client is not connected to any other execution engine.
 */
public class DummyExecEnv extends ExecEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(
      DummyExecEnv.class.getName());

  @Override
  public SessionId connect() {
    // Do nothing.
    return new SessionId(0);
  }

  @Override
  public boolean isConnected() {
    return false; // Never connected.
  }

  @Override
  public String getEnvName() {
    return "dummy";
  }

  @Override
  public QuerySubmitResponse submitQuery(String query, Map<String, String> options) {
    return new QuerySubmitResponse("Not connected", null);
  }

  @Override
  public FlowId addFlow(FlowSpecification spec) {
    LOG.error("Not connected");
    return null;
  }

  @Override
  public void cancelFlow(FlowId id) {
    LOG.error("Not connected");
  }

  @Override
  public Map<FlowId, FlowInfo> listFlows() {
    LOG.error("Not connected");
    return new HashMap<FlowId, FlowInfo>();
  }

  @Override
  public void joinFlow(FlowId id) {
    LOG.error("Not connected");
  }

  @Override
  public boolean joinFlow(FlowId id, long timeout) {
    LOG.error("Not connected");
    return false;
  }

  @Override
  public void watchFlow(SessionId sessionId, FlowId flowId) {
    LOG.error("Not connected");
  }

  @Override
  public void unwatchFlow(SessionId sessionId, FlowId flowId) {
    LOG.error("Not connected");
  }

  @Override
  public List<FlowId> listWatchedFlows(SessionId sessionId) {
    return Collections.emptyList();
  }

  @Override
  public void setFlowName(FlowId flowId, String name) {
    LOG.error("Not connected");
  }

  @Override
  public void disconnect(SessionId sessionId) {
    // Do nothing.
  }
  
  @Override
  public void shutdown() {
    LOG.error("Cannot shut down: Not connected.");
  }
}
