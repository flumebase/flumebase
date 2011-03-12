// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.server;

import org.apache.thrift.TException;

import org.apache.thrift.transport.TTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.thrift.ClientConsole;

import com.odiago.flumebase.util.ClosePublisher;

/**
 * State about a user's session, including the callback RPC connection, etc.
 */
public class UserSession extends ClosePublisher implements ClientConsole.Iface {
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
  public synchronized void close() {
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
  public synchronized void sendInfo(String output) {
    if (null == mClient) {
      // We're already closed. Ignore.
      return;
    }
    try {
      mClient.sendInfo(output);
    } catch (TException te) {
      LOG.error("Could not send data to client: " + te);
      close();
    }
  }

  /**
   * Sends err output to the user's console. If this triggers an error, the session
   * is closed and removed from the list of active sessions.
   */
  public synchronized void sendErr(String output) {
    if (null == mClient) {
      // We're already closed. Ignore.
      return;
    }
    try {
      mClient.sendErr(output);
    } catch (TException te) {
      LOG.error("Could not send data to client: " + te);
      close();
    }
  }
}

