// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import com.odiago.rtengine.thrift.TFlowId;

/**
 * Identifier for a deployed flow within the environment.
 */
public class FlowId implements Comparable<FlowId> {

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

  @Override
  public boolean equals(Object other) {
    if (!other.getClass().equals(getClass())) {
      return false;
    }

    FlowId otherFlow = (FlowId) other;
    return mId == otherFlow.mId;
  }

  @Override
  public int hashCode() {
    return (int) (mId & 0xFFFFFFFF);
  }

  public TFlowId toThrift() {
    return new TFlowId(mId);
  }

  public static FlowId fromThrift(TFlowId other) {
    return new FlowId(other.id);
  }

  @Override
  public int compareTo(FlowId other) {
    if (null == other) {
      return 1;
    } else {
      return Long.valueOf(mId).compareTo(other.mId);
    }
  }
}

