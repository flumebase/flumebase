// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.client;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.FlowInfo;
import com.odiago.rtengine.exec.QuerySubmitResponse;

import com.odiago.rtengine.plan.FlowSpecification;

import com.odiago.rtengine.thrift.RemoteServer;
import com.odiago.rtengine.thrift.TFlowId;
import com.odiago.rtengine.thrift.TFlowInfo;

/**
 * Execution environment that proxies all client requests to a server
 * via a thrift protocol.
 */
public class ThriftClientEnvironment extends ExecEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(
      ThriftClientEnvironment.class.getName());

  private Configuration mConf;

  private String mHost;

  private int mPort;

  /** The socket connection for the RPC. */
  private TTransport mTransport;

  /** The service stub endpoint we interact with. */
  private RemoteServer.Client mClient;

  public ThriftClientEnvironment(Configuration conf, String host, int port)  {
    mConf = conf;
    mHost = host;
    mPort = port;
  }

  @Override
  public void connect() throws IOException {
    try {
      mTransport = new TFramedTransport(new TSocket(mHost, mPort));
      mTransport.open();

      TProtocol protocol = new TBinaryProtocol(mTransport);
      mClient = new RemoteServer.Client(protocol);
    } catch (TException e) {
      mTransport = null;
      mClient = null;
      throw new IOException(e);
    }
  }

  @Override
  public boolean isConnected() {
    return mTransport.isOpen();
  }

  @Override
  public String getEnvName() {
    return "thriftClient";
  }

  @Override
  public QuerySubmitResponse submitQuery(String query) throws IOException {
    try {
      return QuerySubmitResponse.fromThrift(mClient.submitQuery(query));
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public FlowId addFlow(FlowSpecification spec) throws IOException {
    LOG.error("Raw addFlow() is an unsupported operation in ThriftClientEnvironment.");
    return null;
  }

  @Override
  public void cancelFlow(FlowId id) throws IOException {
    try {
      mClient.cancelFlow(id.toThrift());
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public Map<FlowId, FlowInfo> listFlows() throws IOException {
    try {
      Map<TFlowId, TFlowInfo> flows = mClient.listFlows();
      Map<FlowId, FlowInfo> out = new TreeMap<FlowId, FlowInfo>();

      for (Map.Entry<TFlowId, TFlowInfo> entry : flows.entrySet()) {
        out.put(FlowId.fromThrift(entry.getKey()), FlowInfo.fromThrift(entry.getValue()));
      }

      return out;
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public void joinFlow(FlowId id) throws IOException {
    try {
      mClient.joinFlow(id.toThrift(), 0);
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public boolean joinFlow(FlowId id, long timeout) throws IOException {
    try {
      return mClient.joinFlow(id.toThrift(), timeout);
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public void disconnect() throws IOException {
    mTransport.close();
    mClient = null;
  }

  @Override
  public void shutdown() throws IOException {
    try {
      mClient.shutdown();
      disconnect();
    } catch (TException te) {
      throw new IOException(te);
    }
  }
}
