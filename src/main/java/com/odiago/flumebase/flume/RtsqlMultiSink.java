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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;

/**
 * EventSink that receives events from upstream in a Flume pipeline.
 * The EventSink then delivers the events to several different RtsqlSink instances
 * subscribed to this one for delivery in a FlumeBase flow.
 *
 * <p>This is different than an ordinary Flume FanOutSink in that the child
 * sinks can be configured outside of Flume's ordinary workflow.</p>
 *
 * <p>This sink is thread safe.</p>
 */
public class RtsqlMultiSink extends EventSink.Base {
  private static final Logger LOG = LoggerFactory.getLogger(RtsqlMultiSink.class.getName());

  /**
   * Mapping from id strings to RtsqlMultiSink instances. Populated
   * by the RtsqlMultiSink instance by its constructor and close() method. 
   */
  private static final Map<String, RtsqlMultiSink> MULTI_SINK_MAP;
  static {
    MULTI_SINK_MAP = Collections.synchronizedMap(new HashMap<String, RtsqlMultiSink>());
  }

  /**
   * @return the RtsqlMultiSink instance configured for the specified id.
   */
  public static RtsqlMultiSink getMultiSinkInstance(String id) {
    return MULTI_SINK_MAP.get(id);
  }

  private static void bindMultiSinkInstance(String id, RtsqlMultiSink instance)
      throws IOException {
    synchronized (MULTI_SINK_MAP) {
      synchronized (instance) {
        RtsqlMultiSink existingMultiSink = MULTI_SINK_MAP.get(id);
        if (existingMultiSink != null) {
          // Flume may actually create several instances of the same sink in this
          // process, before actually creating the "real" one. The first few will
          // be used internally in the Flume master instance. Only the last one 
          // is "official." Only the last one will be open()'d by the node, but
          // since that's a lazy operation after the first actual event comes in,
          // this prevents us from writing tests in a sane fashion. So any RtsqlSink
          // instances we've registered in a previous MultiSink, we add to the current
          // MultiSink here. We try to prevent this by having our Builder in the
          // FlumePlugin guard against this by checking with the MULTI_SINK_MAP
          // cache first.
          synchronized (existingMultiSink) {
            LOG.debug("Replacing existing RtsqlMultiSink for id=" + id);
            for (Map.Entry<String, RtsqlSink> entry : existingMultiSink.mChildSinks.entrySet()) {
              instance.addChildSink(entry.getKey(), entry.getValue());
            }
            existingMultiSink.removeAll();
          }
        }
        MULTI_SINK_MAP.put(id, instance);
      }
    }
  }

  /**
   * Id string distinguishing this RtsqlMultiSink instance from others
   * in the same process.
   */
  private final String mMultiSinkId;

  /** Mapping from sink ContextSourceName to RtsqlSink instances. */
  private Map<String, RtsqlSink> mChildSinks;

  /**
   * List calculated from mChildSinks. This is used within the append() method
   * so that it does not need to lock mChildSinks every time it is called.
   */
  private List<RtsqlSink> mActiveSinks;

  private boolean mIsOpen;

  public RtsqlMultiSink(String multiSinkId) throws IOException {
    mMultiSinkId = multiSinkId;
    mChildSinks = new HashMap<String, RtsqlSink>();
    mActiveSinks = new LinkedList<RtsqlSink>();
    mIsOpen = false;

    LOG.debug("Created rtsqlmultisink id=" + multiSinkId);
    bindMultiSinkInstance(multiSinkId, this);
  }

  /**
   * Recalculate the list in mActiveSinks for use by append(). Switch out
   * the reference in the minimum time possible, to ensure that we don't
   * need to block in the append method itself.
   */
  private void recalculateActiveSinks() {
    List<RtsqlSink> newActiveSinks = new LinkedList<RtsqlSink>();
    synchronized (this) {
      newActiveSinks.addAll(mChildSinks.values());
      mActiveSinks = newActiveSinks;
      LOG.debug("Recalculated active sink list; activelen=" + mActiveSinks.size());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException {
    LOG.debug("Opening RtsqlMultiSink");

    synchronized (this) {
      for (RtsqlSink childSink : mChildSinks.values()) {
        childSink.open();
      }

      recalculateActiveSinks();
      mIsOpen = true;
    }
  }


  /**
   * Add a child sink to the collection of sinks we deliver to. This method
   * opens the child sink immediately if this sink is itself open.
   */
  public void addChildSink(String contextName, RtsqlSink sink) throws IOException {
    LOG.debug("Attaching child sink for context " + contextName
        + " to multisink id=" + mMultiSinkId);
    synchronized (this) {
      // Precondition: there can't already be a child sink with this context name.
      assert mChildSinks.get(contextName) == null;

      if (mIsOpen) {
        sink.open();
      }

      mChildSinks.put(contextName, sink);
      recalculateActiveSinks();
    }
  }

  /**
   * Removes a child sink from the collection of sinks we deliver to.
   * This method closes the child sink after removing it from the list
   */
  public void removeChildSink(String contextName) throws IOException {
    LOG.debug("Removing child sink for context " + contextName
        + " from multisink id=" + mMultiSinkId);
    synchronized (this) {
      RtsqlSink childSink = mChildSinks.remove(contextName);
      if (null == childSink) {
        LOG.warn("No child sink for context : " + contextName);
        return;
      }

      recalculateActiveSinks();
      childSink.close();
    }
  }

  /**
   * Remove all child sinks. If we are already open, close them all. Otherwise,
   * just throw out the references.
   */
  private void removeAll() throws IOException {
    synchronized (this) {
      if (mIsOpen) {
        for (RtsqlSink child : mChildSinks.values()) {
          child.close();
        }
      } else {
        mChildSinks.clear();
        mActiveSinks = new LinkedList<RtsqlSink>();
      }
    }
  }


  /** {@inheritDoc} */
  @Override
  public void append(Event e) throws IOException {
    List<RtsqlSink> sinks = mActiveSinks;

    for (RtsqlSink sink : sinks) {
      sink.append(new EventImpl(e));
    }
  }

  /** {@inheritDoc) */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      mActiveSinks = new LinkedList<RtsqlSink>();
      for (RtsqlSink sink : mChildSinks.values()) {
        sink.close();
      }
    }

    MULTI_SINK_MAP.remove(mMultiSinkId);
  }
}
