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
