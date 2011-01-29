// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.client;

import java.io.IOException;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;

import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.ExecEnvironment;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.FlowInfo;
import com.odiago.rtengine.exec.QuerySubmitResponse;

import com.odiago.rtengine.plan.FlowSpecification;

import com.odiago.rtengine.server.SessionId;

import com.odiago.rtengine.thrift.CallbackConnectionError;
import com.odiago.rtengine.thrift.ClientConsole;
import com.odiago.rtengine.thrift.RemoteServer;
import com.odiago.rtengine.thrift.TFlowId;
import com.odiago.rtengine.thrift.TFlowInfo;

import com.odiago.rtengine.util.NetUtils;

/**
 * Execution environment that proxies all client requests to a server
 * via a thrift protocol.
 */
public class ThriftClientEnvironment extends ExecEnvironment {
  private static final Logger LOG = LoggerFactory.getLogger(
      ThriftClientEnvironment.class.getName());

  private Configuration mConf;

  /** Remote host to connect to. */
  private String mHost;

  /** Remote port to connect to. */
  private int mPort;

  /** The socket connection for the RPC. */
  private TTransport mTransport;

  /** The service stub endpoint we interact with. */
  private RemoteServer.Client mClient;

  /** id associated with the connection to our session. */
  private SessionId mSessionId;

  /** The TServer hosting the ClientConsole callback service. */
  private TServer mConsoleServer;

  public ThriftClientEnvironment(Configuration conf, String host, int port)  {
    mConf = conf;
    mHost = host;
    mPort = port;
  }

  @Override
  public SessionId connect() throws IOException {
    try {
      // Establish the connection to the server.
      LOG.debug("Connecting to remote server.");
      mTransport = new TFramedTransport(new TSocket(mHost, mPort));
      mTransport.open();

      TProtocol protocol = new TBinaryProtocol(mTransport);
      mClient = new RemoteServer.Client(protocol);

      String consoleHost = null;
      int consolePort = 0;
      try {
        // Start our own server hosting the ClientConsole service, so
        // the server can send us back results.
        ClientConsoleImpl consoleImpl = new ClientConsoleImpl();
        consolePort = mConf.getInt(ClientConsoleImpl.CONSOLE_SERVER_PORT_KEY,
            ClientConsoleImpl.DEFAULT_CONSOLE_SERVER_PORT);
        LOG.debug("Starting ClientConsole service on port " + consolePort);
        TServerTransport consoleTransport = new TServerSocket(consolePort);
        TTransportFactory transportFactory = new TFramedTransport.Factory();
        TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
        TProcessor processor = new ClientConsole.Processor(consoleImpl);
        mConsoleServer = new TSimpleServer(processor, consoleTransport, transportFactory,
            protocolFactory);

        // TODO: Is this technically a race? The Thread.start() method returns
        // when the thread is switched to RUNNING, but there is an arbitrary
        // amount of time before serve() kicks in. TServer does not have any
        // isRunning() equivalent to join on, so we don't know if it's really
        // ready or not.
        LOG.debug("Starting server thread");
        Thread consoleThread = new Thread() {
          public void run() {
            mConsoleServer.serve();
          }
        };
        consoleThread.setDaemon(true);
        consoleThread.start();

        // Tell the server to connect back to us.
        consoleHost = NetUtils.getHostName();
        mSessionId = SessionId.fromThrift(
            mClient.createSession(consoleHost, (short) consolePort));
      } catch (CallbackConnectionError cce) {
        LOG.error("Server could not connect to our console hosted at "
            + consoleHost + ":" + consolePort);
        LOG.error("Flows may be submitted, but results cannot be viewed.");
        if (null != mConsoleServer) {
          // Stop the console server since the server can't connect to it.
          mConsoleServer.stop();
        }
      }
    } catch (TException e) {
      mTransport = null;
      mClient = null;
      throw new IOException(e);
    }

    return mSessionId;
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
  public QuerySubmitResponse submitQuery(String query, Map<String, String> options)
      throws IOException {
    try {
      return QuerySubmitResponse.fromThrift(mClient.submitQuery(query, options));
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
  public void watchFlow(SessionId sessionId, FlowId flowId) throws IOException {
    try {
      mClient.watchFlow(sessionId.toThrift(), flowId.toThrift());
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  @Override
  public void unwatchFlow(SessionId sessionId, FlowId flowId) throws IOException {
    try {
      mClient.unwatchFlow(sessionId.toThrift(), flowId.toThrift());
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
