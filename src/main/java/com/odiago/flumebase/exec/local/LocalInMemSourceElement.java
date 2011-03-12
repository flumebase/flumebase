// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.lang.InterruptedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.FlowElementImpl;
import com.odiago.flumebase.exec.InMemStreamSymbol;
import com.odiago.flumebase.exec.ParsingEventWrapper;

import com.odiago.flumebase.parser.TypedField;

/**
 * Generates events from a pre-populated in-memory buffer.
 * Used for testing flows.
 */
public class LocalInMemSourceElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalInMemSourceElement.class.getName());

  /** The stream generating the events. */
  private InMemStreamSymbol mStreamSymbol;

  /** Fields of the input event. */ 
  private List<String> mFieldNames;

  /** Additional thread that actually drives event generation. */
  private class EventGenThread extends Thread {
    public void run() {
      Iterator<Event> iter = mStreamSymbol.getEvents();
      FlowElementContext context = getContext();
      try {
        // Iterate over all the input events, and wrap them in
        // a parsing EventWrapper; advance these to the output.
        while (iter.hasNext()) {
          Event rawEvent = iter.next();
          rawEvent.set(STREAM_NAME_ATTR, mStreamSymbol.getName().getBytes());
          EventWrapper wrapper = new ParsingEventWrapper(mStreamSymbol.getEventParser(),
              mFieldNames);
          wrapper.reset(rawEvent);
          context.emit(wrapper);
        }
      } catch (IOException ioe) {
        LOG.error("IOException emitting event: " + ioe);
      } catch (InterruptedException ie) {
        LOG.error("Interrupted emitting event: " + ie);
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
      List<TypedField> fields, InMemStreamSymbol streamSymbol) {

    super(context);
    mStreamSymbol = streamSymbol;
    mFieldNames = new ArrayList<String>();
    for (TypedField field : fields) {
      mFieldNames.add(field.getAvroName());
    }
  }

  @Override
  public void takeEvent(EventWrapper e) {
    throw new RuntimeException("LocalInMemSourceElement does not expect takeEvent()");
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
