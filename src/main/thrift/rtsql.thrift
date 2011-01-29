#!/usr/bin/env thrift
## (c) Copyright 2011 Odiago, Inc.

namespace java com.odiago.rtengine.thrift

/**
 * Thrift-serialized version of exec.FlowId.
 * For now, FlowId is just a long integer, but this is held in a struct as it
 * may be broadened to encompass a string name, etc.
 */
struct TFlowId {
  1: required i64 id 
}


/** Thrift-serialized version of exec.QuerySubmitResponse. */
struct TQuerySubmitResponse {
  1: optional string msg,
  2: optional TFlowId flowId
}


/** Thrift version of exec.FlowInfo. */
struct TFlowInfo {
  1: required TFlowId flowId,
  2: required string query
}

/* Thrift version of server.SessionId */
struct TSessionId {
  1: required i64 id
}

/**
 * Thrown when there is an error connecting to or sending data through
 * the client callback RPC interface.
 */
exception CallbackConnectionError {
}

/**
 * RemoteServer defines a service that executes queries over Flume data.
 * Clients connect to this service to submit and monitor long-running queries.
 * This service is used as the backend implementation of an ExecEnvironment as
 * interfaced with by a client.
 */
service RemoteServer {
  /**
   * After connecting to the server, create a sessionId on the server
   * identifying this client--server connection. Provides the server with a
   * host:port pair which is hosting a reverse RPC allowing the server to push
   * per-session info to the client.
   *
   * @throws CallbackConnectionError if a callback connection could not be
   * established on the specified host:port
   */
  TSessionId createSession(1: required string host, 2: required i16 port)
    throws (1:CallbackConnectionError cce),

  /**
   * Submit a query statement to the planner. Returns a human-readable message
   * about its status, as well as an optional flow id if the query was
   * successfully submitted.
   */
  TQuerySubmitResponse submitQuery(1: required string query,
      2: required map<string, string> options),

  /** Cancel a running flow. */
  void cancelFlow(1: required TFlowId id),

  /** Return information about all running flows. */
  map<TFlowId, TFlowInfo> listFlows(),

  /**
   * Wait up to 'timeout' ms for the specified flow to complete.
   * Block indefinitely if timeout is 0 or unspecified.
   */
  bool joinFlow(1: required TFlowId id, 2: optional i64 timeout = 0),

  /**
   * Instruct the server to add this user's session to the output of the
   * specified flow.
   */
  void watchFlow(1: required TSessionId sessionId, 2: required TFlowId flowId),

  /**
   * Instruct the server to remove this user's session from the set of
   * listeners for the specified flow.
   */
  void unwatchFlow(1: required TSessionId sessionId, 2: required TFlowId flowId),

  /**
   * Gather a list of all FlowIds being watched by the specified session.
   */
  list<TFlowId> listWatchedFlows(1: required TSessionId sessionId),

  /** Shut down the remote server. */
  oneway void shutdown()
}


/**
 * ClientConsole defines a service that operates in the reverse direction.
 * The service is hosted by the client, and the remote server connects back
 * to it. When a flow yields output records, the server sends them to the
 * client over this channel, where they are printed to the user's console.
 */
service ClientConsole {
  /** Send information to the client to display. */
  oneway void sendInfo(1: required string info),

  /** Send error information to the client to display (e.g., on stderr) */
  oneway void sendErr(1: required string err)
}
