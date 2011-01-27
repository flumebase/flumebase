// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.server;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.apache.thrift.server.TServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.FlowInfo;

import com.odiago.rtengine.exec.local.LocalEnvironment;

import com.odiago.rtengine.thrift.RemoteServer;
import com.odiago.rtengine.thrift.TFlowId;
import com.odiago.rtengine.thrift.TFlowInfo;
import com.odiago.rtengine.thrift.TQuerySubmitResponse;

/**
 * Implementation of the RemoteServer thrift interface.
 * This is the "guts" of the remote server that translate RPC calls
 * from the client into actions performed on the ExecEnvironment hosted within.
 */
class RemoteServerImpl implements RemoteServer.Iface {
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

  public RemoteServerImpl(Configuration conf) {
    mExecEnv = new LocalEnvironment(conf);
    mStarted = false;
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

  // Implementation of remote API follows.

  @Override
  public TQuerySubmitResponse submitQuery(String query) throws TException {
    try {
      return mExecEnv.submitQuery(query).toThrift();
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
