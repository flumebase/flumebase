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

import java.util.concurrent.BlockingQueue;

import com.cloudera.flume.core.Event;

/** Container for all the state an RtsqlSource needs to lazily initialize. */
public class SourceContext {
  /** Name associated with this source context. */
  private final String mContextName;

  /**
   * Queue of events that will be populated by flumebase; these should be
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
