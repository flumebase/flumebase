// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;
import com.odiago.rtengine.exec.ParsingEventWrapper;

import com.odiago.rtengine.io.DelimitedEventParser;

import com.odiago.rtengine.parser.TypedField;

/**
 * FlowElement providing source data from a local file.
 * The file format is a set of text records with the form:
 * (long integer in base 10 str encoding) \t (string) \n
 *
 * The integer on each line specifies the timestamp of the event.
 */
public class LocalFileSourceElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalFileSourceElement.class.getName());

  private String mFilename;
  private EventGenThread mEventGenThread;
  private volatile boolean mIsFinished;
  private List<String> mFieldNames;

  /**
   * Additional thread that actually reads the file and converts it
   * to events to inject into the flow.
   */
  private class EventGenThread extends Thread {
    public void run() {
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(mFilename));
        while (true) {
          if (mIsFinished) {
            LOG.info("Closing EventGenThread; mIsFinished set to true");
            break;
          }

          String line = reader.readLine();
          if (null == line) {
            LOG.info("Closing EventGenThread; file is complete");
            break;
          }

          if (line.length() == 0) {
            // Ignore empty lines.
            continue;
          }

          int tabIdx = line.indexOf('\t', 0);
          if (-1 == tabIdx) {
            LOG.warn("Invalid input line contains no timestamp field: " + line);
          }

          try {
            long timestamp = Long.parseLong(line.substring(0, tabIdx));
            byte [] body = line.substring(tabIdx + 1).getBytes();
            EventImpl event = new EventImpl(body, timestamp, Priority.INFO, 0, "localhost");
            EventWrapper wrapper = new ParsingEventWrapper(new DelimitedEventParser(),
                mFieldNames);
            wrapper.reset(event);
            emit(wrapper);
          } catch (NumberFormatException nfe) {
            LOG.warn("Could not parse timestamp: " + nfe);
          }
        }
      } catch (InterruptedException ie) {
        LOG.error("Interruption during EventGenThread (suspending): " + ie);
      } catch (IOException ioe) {
        LOG.error("IOException in EventGenThread: " + ioe);
      } finally {
        if (null != reader) {
          try {
            reader.close();
          } catch (IOException ioe) {
            LOG.warn("IOException closing file reader" + ioe);
          }
        }

        try {
          getContext().notifyCompletion();
        } catch (IOException ioe) {
          LOG.warn("IOException notifying flow of file source completion: " + ioe);
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException notifying flow of file source completion: " + ie);
        }
      }
    }
  }

  public LocalFileSourceElement(FlowElementContext context, String fileName,
      List<TypedField> fields) {
    super(context);
    mFilename = fileName;
    mFieldNames = new ArrayList<String>();
    for (TypedField field : fields) {
      mFieldNames.add(field.getName());
    }
  }

  @Override
  public void open() throws IOException {
    if (null != mEventGenThread) {
      throw new IOException("LocalFileSourceElement.open() called multiple times");
    }
    mEventGenThread = new EventGenThread();
    mEventGenThread.start();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    mIsFinished = true;
    mEventGenThread.join();
    super.close();
  }

  @Override
  public void takeEvent(EventWrapper e) {
    // We generate our own events; nothing should be upstream from us.
    throw new RuntimeException("LocalFileSourceElement does not support incoming events");
  }

  @Override
  public String toString() {
    return "FileSource[mFilename=\"" + mFilename + "\"]";
  }
}
