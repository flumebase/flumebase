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

import com.odiago.flumebase.thrift.TSessionId;

/**
 * Identifier for a user session connected to the remote server.
 */
public class SessionId {

  private final long mId;

  public SessionId(long id) {
    mId = id;
  }

  public long getId() {
    return mId;
  }
  
  public String toString() {
    return "session[mId=" + mId + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (!other.getClass().equals(getClass())) {
      return false;
    }

    SessionId otherSession = (SessionId) other;
    return mId == otherSession.mId;
  }

  @Override
  public int hashCode() {
    return (int) (mId & 0xFFFFFFFF);
  }

  public TSessionId toThrift() {
    return new TSessionId(mId);
  }

  public static SessionId fromThrift(TSessionId other) {
    return new SessionId(other.id);
  }
}

