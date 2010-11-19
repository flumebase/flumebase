// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;

import com.odiago.rtengine.exec.FlowElementContext;

/**
 * EventSink that receives events from upstream in a Flume pipeline.
 * The EventSink then injects the events into a FlowElementContext
 * for delivery in an rtengine flow.
 */
public class RtsqlSink extends EventSink.Base {
  private static final Logger LOG = LoggerFactory.getLogger(RtsqlSink.class.getName());

  /**
   * An identifier for the flow and source within a flow we're
   * providing the events for.
   */
  private String mContextSourceName;

  /**
   * The FlowElementContext for our containing "source" FlowElement;
   * where we insert events we receive from Flume.
   */
  private FlowElementContext mWriteContext;

  public RtsqlSink(String contextSourceName) {
    mContextSourceName = contextSourceName;
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException {
    LOG.info("Opening Flume sink for flow/source: " + mContextSourceName);
    mWriteContext = SinkContextBindings.get().getContext(mContextSourceName);
    if (null == mWriteContext) {
      throw new IOException("No context binding available for flow/source: "
          + mContextSourceName);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void append(Event e) throws IOException {
    if (null == mWriteContext) {
      throw new IOException("append() called before open()");
    }

    try {
      // TODO(aaron): In local mode, this appends to an unbounded queue. We should
      // ensure that we can apply some sort of backpressure by blocking if this
      // is actually getting out of hand.
      mWriteContext.emit(e);
    } catch (InterruptedException ie) {
      // TODO(aaron): When Flume's api lets us throw InterruptedException, do so directly.
      throw new IOException(ie);
    }
  }

  /** {@inheritDoc) */
  @Override
  public void close() throws IOException {
    LOG.info("Closing Flume sink for flow/source: " + mContextSourceName);
  }
}
