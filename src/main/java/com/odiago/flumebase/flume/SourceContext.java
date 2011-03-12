// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.flume;

import java.util.concurrent.BlockingQueue;

import com.cloudera.flume.core.Event;

/** Container for all the state an RtsqlSource needs to lazily initialize. */
public class SourceContext {
  /** Name associated with this source context. */
  private final String mContextName;

  /**
   * Queue of events that will be populated by rtsql; these should be
   * broadcast to Flume via the associated RtsqlSource.
   */
  private final BlockingQueue<Event> mEventQueue;

  public SourceContext(String contextName, BlockingQueue<Event> eventQueue) {
    mContextName = contextName;
    mEventQueue = eventQueue;
  }

  public String getContextName() {
    return mContextName;
  }

  public BlockingQueue<Event> getEventQueue() {
    return mEventQueue;
  }
}
