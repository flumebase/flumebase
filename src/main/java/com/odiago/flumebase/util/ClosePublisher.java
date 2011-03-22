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

package com.odiago.flumebase.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class that maintains a list of subscribers who are notified
 * when this object closes.
 */
public abstract class ClosePublisher {
  private List<CloseHandler<ClosePublisher>> mSubscribers;

  public ClosePublisher() {
    mSubscribers = new ArrayList<CloseHandler<ClosePublisher>>();
  }

  /** Notify all subscribers that we are closing. */
  public synchronized void close() {
    for (CloseHandler<ClosePublisher> subscriber : mSubscribers) {
      subscriber.handleClose(this);
    }
  }

  /** Add an object to the list of subcribers to our close notifications. */
  public synchronized void subscribeToClose(CloseHandler<? extends ClosePublisher> subscriber) {
    if (!mSubscribers.contains(subscriber)) {
      mSubscribers.add((CloseHandler<ClosePublisher>) subscriber);
    }
  }

  /** Remove an object from the list of subscribers from our close notifications. */
  public synchronized void unsubscribeFromClose(
      CloseHandler<? extends ClosePublisher> subscriber) {
    mSubscribers.remove(subscriber);
  }
}
