// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.server;

import org.apache.thrift.TException;

import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.thrift.ClientConsole;

import com.odiago.rtengine.util.ClosePublisher;

/**
 * State about a user's session, including the callback RPC connection, etc.
 */
public class UserSession extends ClosePublisher {
  private static final Logger LOG = LoggerFactory.getLogger(
      UserSession.class.getName());

  /** unique SessionId associated with this connection. */
  private final SessionId mSessionId;

  /** Thrift transport for the RPC conn back to the client. */
  private TTransport mRpcTransport;

  /** Thrift service client for the RPC conn back to the client. */
  private ClientConsole.Iface mClient;
  
  public UserSession(SessionId id, TTransport transport, ClientConsole.Iface client) {
    mSessionId = id;
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
    LOG.info("Closing user session: " + mSessionId);

    if (null != mRpcTransport) {
      mRpcTransport.close();
    }

    mRpcTransport = null;
    mClient = null;

    // Notify all subscribers of our close. Server, FlowElements, etc. can
    // remove us from their active lists.
    super.close();
  }

  public SessionId getId() {
    return mSessionId;
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

  /**
   * Sends output to the user's console. If this triggers an error, the session
   * is closed and removed from the list of active sessions.
   */
  public void sendInfo(String output) {
    try {
      mClient.sendInfo(output);
    } catch (TException te) {
      LOG.error("Could not send data to client: " + te);
      close();
    }
  }
}

