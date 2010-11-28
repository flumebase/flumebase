// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.IOException;

import java.lang.InterruptedException;

import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.InMemStreamSymbol;
import com.odiago.rtengine.exec.ParsingFlowElementImpl;

import com.odiago.rtengine.parser.TypedField;

/**
 * Generates events from a pre-populated in-memory buffer.
 * Used for testing flows.
 */
public class LocalInMemSourceElement extends ParsingFlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalInMemSourceElement.class.getName());

  /** The stream generating the events. */
  private InMemStreamSymbol mStreamSymbol;

  /** Additional thread that actually drives event generation. */
  private class EventGenThread extends Thread {
    public void run() {
      Iterator<Event> iter = mStreamSymbol.getEvents();
      try {
        // Iterate over all the input events, and push them through
        // ParsingFlowElementImpl's takeEvent(), which will parse them
        // into Avro records and work from there.
        while (iter.hasNext()) {
          Event rawEvent = iter.next();
          takeEvent(rawEvent);
        }
      } catch (IOException ioe) {
        LOG.error("IOException replaying events: " + ioe);
      } catch (InterruptedException ie) {
        LOG.error("InterruptedException replaying events: " + ie);
      } finally {
        try {
          getContext().notifyCompletion();
        } catch (IOException ioe) {
          LOG.warn("IOException sending completion notice: " + ioe);
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException sending completion notice: " + ie);
        }
      }
    }
  }

  private EventGenThread mEventGenThread;

  public LocalInMemSourceElement(FlowElementContext context,
      Schema outputSchema, List<TypedField> fields, InMemStreamSymbol streamSymbol) {

    super(context, outputSchema, fields);
    mStreamSymbol = streamSymbol;
  }

  @Override
  public void open() throws IOException, InterruptedException {
    super.open();
    if (null != mEventGenThread) {
      throw new IOException("LocalInMemSourceElement.open() called multiple times");
    }
    mEventGenThread = new EventGenThread();
    mEventGenThread.start();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    mEventGenThread.join();
    super.close();
  }

  @Override
  public String toString() {
    return "InMemStreamSource[mStream=\"" + mStreamSymbol.getName() + "\"]";
  }
}
