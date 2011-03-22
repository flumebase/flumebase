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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.odiago.flumebase.exec.local.LocalEnvironment;

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
