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

package com.odiago.flumebase.server;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.apache.thrift.server.TServer;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.ExecEnvironment;
import com.odiago.flumebase.exec.FlowId;
import com.odiago.flumebase.exec.FlowInfo;

import com.odiago.flumebase.thrift.CallbackConnectionError;
import com.odiago.flumebase.thrift.ClientConsole;
import com.odiago.flumebase.thrift.RemoteServer;
import com.odiago.flumebase.thrift.TFlowId;
import com.odiago.flumebase.thrift.TFlowInfo;
import com.odiago.flumebase.thrift.TQuerySubmitResponse;
import com.odiago.flumebase.thrift.TSessionId;

import com.odiago.flumebase.util.CloseHandler;

/**
 * Implementation of the RemoteServer thrift interface.
 * This is the "guts" of the remote server that translate RPC calls
 * from the client into actions performed on the ExecEnvironment hosted within.
 */
class RemoteServerImpl implements RemoteServer.Iface, CloseHandler<UserSession> {
  private static final Logger LOG = LoggerFactory.getLogger(
      RemoteServerImpl.class.getName());

  /**
   * ExecEnvironment within the server -- this is where queries are actually
   * processed.
   */
  private ExecEnvironment mExecEnv;

  /** Thrift server that's providing access to this RemoteServerImpl. */
  private TServer mThriftServer;

  private boolean mStarted; 

  private long mNextSessionId;

  /**
   * Map of sessionId to state about the user's session.
   */
  private Map<SessionId, UserSession> mActiveSessions;

  public RemoteServerImpl(Configuration conf) {
    mStarted = false;
    mNextSessionId = 0;
    mActiveSessions = Collections.synchronizedMap(new HashMap<SessionId, UserSession>());
    mExecEnv = new WorkerEnvironment(conf, mActiveSessions);
  }

  // Methods to manage the server itself.
  
  void start() throws IOException, InterruptedException {
    mExecEnv.connect(); // Starts the local environment thread.
    mStarted = true;
  }

  boolean isRunning() {
    return mStarted;
  }

  void setServer(TServer server) {
    mThriftServer = server;
  }

  /** Indicate to the server that the specified session is dead. */
  private void removeSession(SessionId id) {
    mActiveSessions.remove(id);
  }

  @Override
  public void handleClose(UserSession session) {
    removeSession(session.getId());
  }

  // Implementation of remote API follows.

  @Override
  public TSessionId createSession(String host, short port)
      throws CallbackConnectionError, TException {
    SessionId sessionId = new SessionId(mNextSessionId++);

    // Try to connect to the user's host:port. If host is the empty string
    // or null, do not connect back.
    if (host != null && !"".equals(host)) {
      LOG.info("Assigning session id " + sessionId + " to connection to " + host + ":" + port);

      try {
        TTransport transport = new TFramedTransport(new TSocket(host, port));
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        ClientConsole.Client client = new ClientConsole.Client(protocol);

        // Store the info about this RPC connection in the active sessions table.
        UserSession session = new UserSession(sessionId, transport, client);
        session.subscribeToClose(this);
        mActiveSessions.put(sessionId, session);
      } catch (TException te) {
        LOG.error("Could not create callback RPC connection to " + host + ":" + port);
        throw new CallbackConnectionError();
      }
    }

    return sessionId.toThrift();
  }

  @Override
  public void closeSession(TSessionId tSessionId) throws TException {
    SessionId sessionId = SessionId.fromThrift(tSessionId);
    UserSession userSession = mActiveSessions.get(sessionId);
    if (null == userSession) {
      LOG.error("closeSession() for sessionId " + sessionId + ": no such session.");
    } else {
      userSession.close();
    }
  }

  @Override
  public TQuerySubmitResponse submitQuery(String query, Map<String, String> options)
      throws TException {
    try {
      return mExecEnv.submitQuery(query, options).toThrift();
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void cancelFlow(TFlowId id) throws TException {
    if (null == id) {
      throw new TException("cancelFlow() requires non-null id");
    }

    try {
      mExecEnv.cancelFlow(FlowId.fromThrift(id));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<TFlowId, TFlowInfo> listFlows() throws TException {
    try {
      Map<FlowId, FlowInfo> flows = mExecEnv.listFlows();
      Map<TFlowId, TFlowInfo> out = new HashMap<TFlowId, TFlowInfo>();

      for (Map.Entry<FlowId, FlowInfo> entry : flows.entrySet()) {
        out.put(entry.getKey().toThrift(), entry.getValue().toThrift());
      }

      return out;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean joinFlow(TFlowId id, long timeout) throws TException {
    if (null == id) {
      throw new TException("joinFlow() requires non-null id");
    }

    try {
      return mExecEnv.joinFlow(FlowId.fromThrift(id), timeout);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void watchFlow(TSessionId sessionId, TFlowId flowId) throws TException {
    try {
      mExecEnv.watchFlow(SessionId.fromThrift(sessionId), FlowId.fromThrift(flowId));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void unwatchFlow(TSessionId sessionId, TFlowId flowId) throws TException {
    try {
      mExecEnv.unwatchFlow(SessionId.fromThrift(sessionId), FlowId.fromThrift(flowId));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public List<TFlowId> listWatchedFlows(TSessionId sessionId) throws TException {
    try {
      List<FlowId> flowList = mExecEnv.listWatchedFlows(SessionId.fromThrift(sessionId));
      List<TFlowId> outList = new ArrayList<TFlowId>();
      for (FlowId flowId : flowList) {
        outList.add(flowId.toThrift());
      }
      return outList;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void setFlowName(TFlowId flowId, String name) throws TException {
    try {
      mExecEnv.setFlowName(FlowId.fromThrift(flowId), name);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void shutdown() throws TException {
    try {
      mExecEnv.shutdown(); // Shuts down the local environment.
      if (null == mThriftServer) {
        LOG.warn("Thrift service not registered with RemoteServerImpl; cannot stop serving.");
      } else {
        mThriftServer.stop();
      }
      mStarted = false;
    } catch (Exception e) {
      throw new TException(e);
    }
  }
}
