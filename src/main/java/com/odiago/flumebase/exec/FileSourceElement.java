/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.exec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.FlowElementImpl;
import com.odiago.flumebase.exec.ParsingEventWrapper;
import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.lang.Timestamp;
import com.odiago.flumebase.lang.Type;

import com.odiago.flumebase.parser.TypedField;

/**
 * FlowElement providing source data from a file.
 * The file format is a set of text records with the form:
 * (long integer in base 10 str encoding) \t (string) \n
 *
 * The integer on each line specifies the timestamp of the event.
 */
public class FileSourceElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      FileSourceElement.class.getName());

  /**
   * EVENT FORMAT property specifying which column of the input file is to be
   * used as the timestamp for each event. If left unspecified, the current
   * system time is used for the timestamp of the row, when each row is read
   * from the file.
   */
  public static final String TIMESTAMP_COL_KEY = "timestamp.col";

  private String mFilename;
  private boolean mLocal;
  private EventGenThread mEventGenThread;
  private volatile boolean mIsFinished;

  /** List of all typed fields defined in the stream, with their avro-name mappings, etc. */
  private List<TypedField> mFields;

  /** List of all avro names of the fields in the stream, in the same order as mFields. */
  private List<String> mFieldNames;

  private StreamSymbol mStream;

  /** Private extension of EventImpl that allows us to call setTimestamp(). */
  private static class FileSourceEvent extends EventImpl {
    public FileSourceEvent(byte[] body) {
      super(body, 0, Priority.INFO, 0, "localhost");
    }

    public void setTimestamp(long timestamp) {
      super.setTimestamp(timestamp);
    }
  }

  /**
   * Additional thread that actually reads the file and converts it
   * to events to inject into the flow.
   */
  private class EventGenThread extends Thread {
    public void run() {
      // If the user has specified a column to extract the timestamp from, get it here.
      String timestampCol = mStream.getFormatSpec().getParam(TIMESTAMP_COL_KEY);
      TypedField timestampField = null;
      if (null != timestampCol) {
        // timestampCol refers to a user-selected name for the column. Translate that
        // to the internal ("avro") name for the column.
        for (TypedField field : mFields) {
          if (field.getUserAlias().equals(timestampCol)) {
            timestampField = field;
            break;
          }
        }

        if (null == timestampField) {
          LOG.warn("Could not find column '" + timestampCol + "' to use for timestamps.");
          LOG.warn("Timestamps will be generated based on the local system clock.");
        } else if (!timestampField.getType().getPrimitiveTypeName()
            .equals(Type.TypeName.TIMESTAMP)) {
          LOG.warn("Specified timestamp.col '" + timestampCol + "' has type "
              + timestampField.getType() + ", but we need TIMESTAMP.");
          LOG.warn("Timestamps will be generated based on the local system clock.");
          timestampField = null;
        } else {
          // Ensure that we normalize the type associated with this column for ts retrieval.
          timestampField = new TypedField(timestampField.getAvroName(),
              Type.getNullable(Type.TypeName.TIMESTAMP));
        }
      }

      BufferedReader reader = null;
      try {
        // TODO: Inherit from a global configuration.
        Configuration conf = new Configuration();
        FileSystem fs;
        if (mLocal) {
          fs = FileSystem.getLocal(conf);
        } else {
          fs = FileSystem.get(conf);
        }

        reader = new BufferedReader(new InputStreamReader(fs.open(new Path(mFilename))));
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

          try {
            FileSourceEvent event = new FileSourceEvent(line.getBytes());
            event.set(STREAM_NAME_ATTR, mStream.getName().getBytes());
            ParsingEventWrapper wrapper = new ParsingEventWrapper(mStream.getEventParser(),
                mFieldNames);
            wrapper.reset(event);

            if (timestampField == null) {
              event.setTimestamp(System.currentTimeMillis());
            } else {
              Timestamp timestamp = (Timestamp) wrapper.getField(timestampField);
              if (null == timestamp) {
                event.setTimestamp(System.currentTimeMillis());
              } else {
                event.setTimestamp(timestamp.milliseconds);
              }
            }
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

  public FileSourceElement(FlowElementContext context, String fileName, boolean local,
      List<TypedField> fields, StreamSymbol streamSym) {
    super(context);
    mFilename = fileName;
    mLocal = local;
    mFields = fields;
    mFieldNames = new ArrayList<String>();
    mStream = streamSym;
    for (TypedField field : fields) {
      mFieldNames.add(field.getAvroName());
    }
  }

  @Override
  public void open() throws IOException {
    if (null != mEventGenThread) {
      throw new IOException("FileSourceElement.open() called multiple times");
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
    throw new RuntimeException("FileSourceElement does not support incoming events");
  }

  @Override
  public String toString() {
    return "FileSource[mFilename=\"" + mFilename + "\", mLocal=" + mLocal + "]";
  }
}
