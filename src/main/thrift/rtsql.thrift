#!/usr/bin/env thrift
## (c) Copyright 2011 Odiago, Inc.

namespace java com.odiago.rtengine.thrift

# Thrift-serialized version of exec.FlowId.
# For now, FlowId is just a long integer, but this is held in a struct as it
# may be broadened to encompass a string name, etc.
struct TFlowId {
  1: required i64 id 
}


# Thrift-serialized version of exec.QuerySubmitResponse.
struct TQuerySubmitResponse {
  1: optional string msg,
  2: optional TFlowId flowId
}


# Thrift version of exec.FlowInfo.
struct TFlowInfo {
  1: required TFlowId flowId,
  2: required string query
}


# RemoteServer defines a service that executes queries over Flume data.
# Clients connect to this service to submit and monitor long-running queries.
# This service is used as the backend implementation of an ExecEnvironment as
# interfaced with by a client.
service RemoteServer {
  # Submit a query statement to the planner. Returns a human-readable message
  # about its status, as well as an optional flow id if the query was
  # successfully submitted.
  TQuerySubmitResponse submitQuery(1: required string query),

  # Cancel a running flow.
  void cancelFlow(1: required TFlowId id),

  # Return information about all running flows.
  map<TFlowId, TFlowInfo> listFlows(),

  # Wait up to 'timeout' ms for the specified flow to complete.
  # Block indefinitely if timeout is 0 or unspecified.
  bool joinFlow(1: required TFlowId id, 2: optional i64 timeout = 0),

  # Shut down the remote server.
  oneway void shutdown()
}

