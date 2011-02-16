// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec;

/**
 * Bucket within an aggregate time window.
 */
public class Bucket<BUCKETSTATE> {
  // State held by the aggregate function about this bucket.
  private BUCKETSTATE mState;

  public BUCKETSTATE getState() {
    return mState;
  }

  public void setState(BUCKETSTATE state) {
    mState = state;
  }
}
