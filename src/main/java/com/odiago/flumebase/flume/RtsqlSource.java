// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.flume;

import java.io.IOException;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;

/**
 * EventSource that broadcasts events representing the output of a rtsql flow.
 */
public class RtsqlSource extends EventSource.Base {
  private static final Logger LOG = LoggerFactory.getLogger(RtsqlSource.class.getName());

  /**
   * An identifier for the flow we're broadcasting the results of.
   */
  private String mContextName;

  /**
   * The container for all the state initialized elsewhere in the engine
   * required for processing events at this source.
   */
  private SourceContext mSourceContext;

  /** Queue of events being delivered by flumebase that we should emit as a source. */
  private BlockingQueue<Event> mEventQueue;


  public RtsqlSource(String contextName) {
    mContextName = contextName;
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException {
    LOG.info("Opening Flume source for flow output: " + mContextName);
    mSourceContext = SourceContextBindings.get().getContext(mContextName);
    if (null == mSourceContext) {
      throw new IOException("No context binding available for flow/source: "
          + mContextName);
    }

    mEventQueue = mSourceContext.getEventQueue();
  }

  /** {@inheritDoc} */
  @Override
  public Event next() throws IOException, InterruptedException {
    if (null == mSourceContext) {
      throw new IOException("next() called before open()");
    }

    return mEventQueue.take();
  }

  /** {@inheritDoc) */
  @Override
  public void close() throws IOException {
    LOG.info("Closing Flume source for flow/source: " + mContextName);
    mSourceContext = null;
    mEventQueue = null;
  }
}
