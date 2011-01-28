// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.server;

import com.odiago.rtengine.thrift.TSessionId;

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

