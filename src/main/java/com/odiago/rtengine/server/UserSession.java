// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.server;

import org.apache.thrift.transport.TTransport;

import com.odiago.rtengine.thrift.ClientConsole;

/**
 * State about a user's session, including the callback RPC connection, etc.
 */
public class UserSession {
  /** unique SessionId associated with this connection. */
  private final SessionId mSessionId;

  /** Server hosting this session. */
  private final RemoteServerImpl mServer;

  /** Thrift transport for the RPC conn back to the client. */
  private TTransport mRpcTransport;

  /** Thrift service client for the RPC conn back to the client. */
  private ClientConsole.Iface mClient;
  
  public UserSession(SessionId id, RemoteServerImpl server,
      TTransport transport, ClientConsole.Iface client) {
    mSessionId = id;
    mServer = server;
    mRpcTransport = transport;
    mClient = client;

    assert null != id;
    assert null != mClient;
  }

  /**
   * Close the callback RPC connection and release any resources associated
   * with this client. Remove the client from the server's active sessions
   * table.
   */
  public void close() {
    if (null != mRpcTransport) {
      mRpcTransport.close();
    }

    mRpcTransport = null;
    mClient = null;

    if (null != mServer) {
      mServer.removeSession(mSessionId);
    }
  }

  public ClientConsole.Iface getConsole() {
    return mClient;
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    } else if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    UserSession other = (UserSession) otherObj;
    return mSessionId.equals(other.mSessionId);
  }
}

