// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.plan.FlowSpecification;

import com.odiago.rtengine.server.SessionId;

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
  public void disconnect() {
    // Do nothing.
  }
  
  @Override
  public void shutdown() {
    LOG.error("Cannot shut down: Not connected.");
  }
}
