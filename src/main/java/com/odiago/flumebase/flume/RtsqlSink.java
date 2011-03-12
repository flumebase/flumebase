// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.flume;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElement;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.ParsingEventWrapper;
import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.io.DelimitedEventParser;

import com.odiago.flumebase.parser.TypedField;

/**
 * EventSink that receives events from upstream in a Flume pipeline.
 * The EventSink then injects the events into a FlowElementContext
 * for delivery in a FlumeBase flow.
 */
public class RtsqlSink extends EventSink.Base {
  private static final Logger LOG = LoggerFactory.getLogger(RtsqlSink.class.getName());

  /**
   * An identifier for the flow and source within a flow we're
   * providing the events for.
   */
  private String mContextSourceName;

  /**
   * The container for all the state initialized elsewhere in the engine
   * required for processing events at this sink.
   */
  private SinkContext mSinkContext;

  /**
   * The FlowElementContext for our containing "source" FlowElement;
   * where we insert events we receive from Flume.
   */
  private FlowElementContext mWriteContext;

  /**
   * List of field names contained in each element.
   */
  private List<String> mFieldNames;

  /** Symbol associated with the stream we are the source for. */
  private StreamSymbol mStreamSymbol;

  public RtsqlSink(String contextSourceName) {
    mContextSourceName = contextSourceName;
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException {
    LOG.info("Opening Flume sink for flow/source: " + mContextSourceName);
    mSinkContext = SinkContextBindings.get().getContext(mContextSourceName);
    if (null == mSinkContext) {
      throw new IOException("No context binding available for flow/source: "
          + mContextSourceName);
    }
    mFieldNames = new ArrayList<String>();
    mWriteContext = mSinkContext.getFlowElementContext();
    mStreamSymbol = mSinkContext.getStreamSymbol();
    for (TypedField field : mSinkContext.getFieldTypes()) {
      mFieldNames.add(field.getAvroName());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void append(Event e) throws IOException {
    if (null == mWriteContext) {
      throw new IOException("append() called before open()");
    }

    try {
      e.set(FlowElement.STREAM_NAME_ATTR, mStreamSymbol.getName().getBytes());
      EventWrapper wrapper = new ParsingEventWrapper(mStreamSymbol.getEventParser(),
          mFieldNames);
      wrapper.reset(e);
      mWriteContext.emit(wrapper);
    } catch (InterruptedException ie) {
      // TODO(aaron): When Flume's api lets us throw InterruptedException, do so directly.
      throw new IOException(ie);
    }
  }

  /** {@inheritDoc) */
  @Override
  public void close() throws IOException {
    LOG.info("Closing Flume sink for flow/source: " + mContextSourceName);
    try {
      mWriteContext.notifyCompletion();
    } catch (InterruptedException ie) {
      throw new IOException(ie); // TODO - don't wrap this.
    }

    mWriteContext = null;
  }
}
