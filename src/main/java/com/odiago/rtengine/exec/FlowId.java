// (c) Copyright 2010 Odiago, Inc.


package com.odiago.rtengine.exec;

/**
 * Identifier for a deployed flow within the environment.
 */
public class FlowId {

  private final long mId;

  public FlowId(long id) {
    mId = id;
  }

  public long getId() {
    return mId;
  }
  
  public String toString() {
    return "flow[mId=" + mId + "]";
  }
}

