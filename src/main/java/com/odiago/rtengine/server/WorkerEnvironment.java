// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.server;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.odiago.rtengine.exec.local.LocalEnvironment;

/**
 * ExecEnvironment that performs the actual work inside a remote server.
 * This inherits the main thread loop, etc. of the LocalEnvironment, but
 * also allows the server to inject and remove sessions as they are created
 * and destroyed by clients.
 */
public class WorkerEnvironment extends LocalEnvironment {

  /**
   * Table of active sessions. This is populated and maintained by an external
   * agent; we just use it for lookup purposes. This map must be synchronized.
   */
  private Map<SessionId, UserSession> mSessions;

  public WorkerEnvironment(Configuration conf, Map<SessionId, UserSession> sessionMap) {
    super(conf);
    mSessions = sessionMap;
  }

  @Override
  protected UserSession getSession(SessionId id) {
    return mSessions.get(id);
  }
}
