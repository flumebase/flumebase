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

import com.odiago.flumebase.thrift.TFlowId;

/**
 * Identifier for a deployed flow within the environment.
 */
public class FlowId implements Comparable<FlowId> {

  private final long mId;

  public FlowId(long id) {
    mId = id;
  }

  public long getId() {
    return mId;
  }
  
  public String toString() {
    return "flow[mId=" + mId + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (!other.getClass().equals(getClass())) {
      return false;
    }

    FlowId otherFlow = (FlowId) other;
    return mId == otherFlow.mId;
  }

  @Override
  public int hashCode() {
    return (int) (mId & 0xFFFFFFFF);
  }

  public TFlowId toThrift() {
    return new TFlowId(mId);
  }

  public static FlowId fromThrift(TFlowId other) {
    return new FlowId(other.id);
  }

  @Override
  public int compareTo(FlowId other) {
    if (null == other) {
      return 1;
    } else {
      return Long.valueOf(mId).compareTo(other.mId);
    }
  }
}

