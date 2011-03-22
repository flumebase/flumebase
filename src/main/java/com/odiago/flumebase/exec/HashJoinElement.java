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

import java.io.IOException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.lang.TimeSpan;

import com.odiago.flumebase.parser.TypedField;
import com.odiago.flumebase.parser.WindowSpec;

import com.odiago.flumebase.plan.HashJoinNode;

import com.odiago.flumebase.util.WindowedHashMap;

/**
 * FlowElement that performs a hash join between two input streams
 * based on equality of a specific input key.
 */
public class HashJoinElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      HashJoinElement.class.getName());

  /**
   * HashMap containing enqueued elements of the left stream within the
   * current window.
   */
  private WindowedHashMap<Object, EventWrapper, Long> mLeftMap;

  /**
   * HashMap containing enqueued elements of the right stream within the
   * current window.
   */
  private WindowedHashMap<Object, EventWrapper, Long> mRightMap;

  /** Name of the left-side stream. */
  private String mLeftName; 

  /** Name of the right-side stream. */
  private String mRightName;

  /** Name of the key field from the left stream. */
  private TypedField mLeftKey;

  /** Name of the key field from the right stream. */
  private TypedField mRightKey;

  /** Window specification in which we are joining. */
  private WindowSpec mWindowWidth;

  /**
   * The actual time interval over which we're doing the join; derived
   * from mWindowWidth.
   */
  private TimeSpan mTimeSpan;

  /** Name of the output stream. */
  private String mOutName;

  /**
   * Mapping from field names to indices in CompositeEventWrapper arrays
   * describing the output events from this join operation. 
   */
  private Map<String, Integer> mFieldMap;

  /**
   * The amount of slack time we provide before we evict old elements.
   */
  private int mSlackTime;

  public HashJoinElement(FlowElementContext ctxt, String leftName, String rightName,
      TypedField leftKey, TypedField rightKey, WindowSpec windowWidth, String outName,
      List<TypedField> leftFieldNames, List<TypedField> rightFieldNames, Configuration conf) {
    super(ctxt);

    mSlackTime = conf.getInt(BucketedAggregationElement.SLACK_INTERVAL_KEY,
        BucketedAggregationElement.DEFAULT_SLACK_INTERVAL);
    if (mSlackTime < 0) {
      mSlackTime = BucketedAggregationElement.DEFAULT_SLACK_INTERVAL;
    }

    mLeftMap = new WindowedHashMap<Object, EventWrapper, Long>();
    mRightMap = new WindowedHashMap<Object, EventWrapper, Long>();

    mLeftName = leftName;
    mRightName = rightName;
    mLeftKey = leftKey;
    mRightKey = rightKey;
    mWindowWidth = windowWidth;
    try {
      assert mWindowWidth.getRangeSpec().isConstant();
      mTimeSpan = (TimeSpan) mWindowWidth.getRangeSpec().eval(new EmptyEventWrapper());
    } catch (IOException ioe) {
      // This should be a constant expression, so this would be quite surprising.
      LOG.error("Unexpected IOE during timespan eval() in HashJoin: " + ioe);
    }
    mOutName = outName;

    initFieldMap(leftFieldNames, rightFieldNames);
  }

  public HashJoinElement(FlowElementContext ctxt, HashJoinNode joinNode) {
    this(ctxt, joinNode.getLeftName(), joinNode.getRightName(), joinNode.getLeftKey(),
        joinNode.getRightKey(), joinNode.getWindowWidth(), joinNode.getOutputName(),
        joinNode.getLeftFields(), joinNode.getRightFields(), joinNode.getConf());
  }


  /**
   * Initialize the map we install in every output CompositeEventWrapper.
   * This describes which of the nested EventWrappers contains each field of
   * the joined record. We compute this once and then reuse it in each output
   * event; we always use the ordered list [leftStream, rightStream] in the wrapped
   * list.
   */
  private void initFieldMap(List<TypedField> leftFields, List<TypedField> rightFields) {
    mFieldMap = new HashMap<String, Integer>();

    // Left EventWrapper has index 0...
    for (TypedField f : leftFields) {
      mFieldMap.put(f.getAvroName(), 0);
    }

    // Right EventWrapper has index 1.
    for (TypedField f : rightFields) {
      mFieldMap.put(f.getAvroName(), 1);
    }

    mFieldMap = Collections.unmodifiableMap(mFieldMap);
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    Event event = e.getEvent();

    // Determine which stream the event is from; this determines which map we
    // place the event in, and which map we check for candidate join matches.
    String streamName = e.getAttr(STREAM_NAME_ATTR);
    if (null == streamName) {
      // We don't know which stream this came from. Don't process it.
      LOG.warn("Got event with no " + STREAM_NAME_ATTR + " attribute!");
      return;
    }

    WindowedHashMap<Object, EventWrapper, Long> insertMap; // Map where we insert this event.
    WindowedHashMap<Object, EventWrapper, Long> joinMap; // Map we pull join candidates from.
    TypedField keyField; // The field to grab from the event wrapper.
    boolean isLeft;

    if (streamName.equals(mLeftName)) {
      insertMap = mLeftMap;
      joinMap = mRightMap;
      keyField = mLeftKey;
      isLeft = true;
    } else if (streamName.equals(mRightName)) {
      insertMap = mRightMap;
      joinMap = mLeftMap;
      keyField = mRightKey;
      isLeft = false;
    } else {
      // Not from either stream?
      LOG.warn("Got event with unexpected " + STREAM_NAME_ATTR + "=" + streamName);
      return; // Don't know what to do with this.
    }

    // Look up elements from the opposite map to determine what joins we can perform.
    Object key = e.getField(keyField);
    if (null == key) {
      // The key field is null; this will not match to anything in an inner join.
      return;
    }

    assert mTimeSpan.isRelative;
    long curTime = event.getTimestamp();
    Long lo;
    Long hi;

    if (isLeft) {
      // If this event is from the left stream, calculate the relative time interval normally. 
      lo = curTime + mTimeSpan.lo;
      hi = curTime + mTimeSpan.hi;
    } else {
      // If this event is from the right stream, use the "mirror image" of the timespan.
      // "RANGE INTERVAL 10 MINUTES PRECEDING" actually means, join with the /next/ 10
      // minutes of data from this perspective.
      lo = curTime - mTimeSpan.hi;
      hi = curTime - mTimeSpan.lo;
    }

    LOG.debug("Working on key: " + key + ", isLeft=" + isLeft);
    LOG.debug("Timestamp=" + curTime + ", interval=" + lo + ", " + hi);

    // Join with all the events in the window.
    List<EventWrapper> joinEvents = joinMap.getRange(key, lo, hi, isLeft, !isLeft);
    for (EventWrapper joinWrapper : joinEvents) {
      CompositeEvent outEvent = new CompositeEvent(mFieldMap,
          event.getPriority(), event.getTimestamp(), event.getNanos(), event.getHost());
      CompositeEventWrapper outWrapper = new CompositeEventWrapper();
      if (isLeft) {
        outEvent.add(e);
        outEvent.add(joinWrapper);
      } else {
        // Add the left event to the composite first.
        // Order matters due to the fixed mFieldMap.
        outEvent.add(joinWrapper);
        outEvent.add(e);
      }
      outEvent.setAttr(STREAM_NAME_ATTR, mOutName); // set the output stream name.
      outWrapper.reset(outEvent);
      emit(outWrapper);
    }

    // Save the event for joining with other events that arrive in the future.
    insertMap.put(key, e, curTime);

    // Remove entries from the join target map that are behind the current
    // window, to keep the window maps from overfilling.
    // Anything behind the 'lo' value can be removed.
    joinMap.removeOlderThan(lo - mSlackTime);

    // If we get lots of records on one side of the join but no records
    // on the other side for an extended period of time, we won't be culling the
    // correct map. Given 'lo' calculated from the perspective of oldest entry in
    // the other map, remove obsolete values from insertMap. Calculating based
    // on the oldest entry in the other map ensures that we are not discarding
    // values that we cannot process yet because one stream is delayed.
    Long oldestInOtherMap = joinMap.oldestTimestamp();
    if (null != oldestInOtherMap) {
      Long otherMapLo;
      if (isLeft) {
        otherMapLo = oldestInOtherMap - mTimeSpan.hi;
      } else {
        otherMapLo = oldestInOtherMap + mTimeSpan.lo;
      }
      LOG.debug("otherMapLo=" + otherMapLo);
      insertMap.removeOlderThan(otherMapLo - mSlackTime);
    }
  }
}
