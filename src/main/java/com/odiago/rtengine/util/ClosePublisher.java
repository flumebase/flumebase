// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class that maintains a list of subscribers who are notified
 * when this object closes.
 */
public abstract class ClosePublisher {
  private List<CloseHandler<ClosePublisher>> mSubscribers;

  public ClosePublisher() {
    mSubscribers = new ArrayList<CloseHandler<ClosePublisher>>();
  }

  /** Notify all subscribers that we are closing. */
  public synchronized void close() {
    for (CloseHandler<ClosePublisher> subscriber : mSubscribers) {
      subscriber.handleClose(this);
    }
  }

  /** Add an object to the list of subcribers to our close notifications. */
  public synchronized void subscribeToClose(CloseHandler<? extends ClosePublisher> subscriber) {
    if (!mSubscribers.contains(subscriber)) {
      mSubscribers.add((CloseHandler<ClosePublisher>) subscriber);
    }
  }

  /** Remove an object from the list of subscribers from our close notifications. */
  public synchronized void unsubscribeFromClose(
      CloseHandler<? extends ClosePublisher> subscriber) {
    mSubscribers.remove(subscriber);
  }
}
