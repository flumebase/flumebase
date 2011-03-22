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

package com.odiago.flumebase.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Queue implementation backed by a fixed-size array.
 * enqueue() operations block until space is available.
 */
public class ArrayBoundedSelectableQueue<T> extends SelectableQueue<T> {

  private final int mMaxLen; /** Max number of items the queue can hold. */

  private final Object[] mArray; /** The queue itself. */

  /** Offset of the next item to be dequeued. */
  private int mDequeueOff;

  /** Offset of the next item to be enqueued. */
  private int mEnqueueOff;

  /** Number of elements in the queue. */
  private final AtomicInteger mSize;

  public ArrayBoundedSelectableQueue(int maxLen) {
    mDequeueOff = 0;
    mEnqueueOff = 0;
    mMaxLen = maxLen;
    assert mMaxLen > 0;
    mArray = new Object[mMaxLen];
    mSize = new AtomicInteger(0);
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    return mSize.get();
  }

  /**
   * Given that our internal state allows for a dequeue, perform the operation.
   */
  private T doDequeue() {
    synchronized (this) {
      mSize.decrementAndGet();
      T val = (T) mArray[mDequeueOff++];
      if (mDequeueOff >= mMaxLen) {
        mDequeueOff = 0;
      }

      // Notify a writer that they may operate if need be.
      this.notify();

      return val;
    }
  }

  /** {@inheritDoc} */
  @Override
  public T take() throws InterruptedException {
    synchronized (this) {
      while (mSize.get() == 0) {
        this.wait();
      }

      return doDequeue();
    }
  }

  /** {@inheritDoc} */
  @Override
  public T poll() throws EmptyException {
    synchronized (this) {
      if (mSize.get() == 0) {
        throw new EmptyException();
      }

      return doDequeue();
    }
  }

  /**
   * Enqueue t in the list, assuming we have already guaranteed available space.
   * Does not perform the requisite reader notification, since that must be
   * done under separate monitors to avoid lock inversion.
   */
  private void doEnqueue(T t) {
    synchronized (this) {
      mSize.incrementAndGet();
      mArray[mEnqueueOff++] = t;
      if (mEnqueueOff >= mMaxLen) {
        mEnqueueOff = 0;
      }

      // Notify a reader that we're ready.
      this.notify();
      notifyReaders();
    }
  }

  /** {@inheritDoc} */
  @Override
  public void put(T t) throws InterruptedException {
    synchronized (this) {
      while (mSize.get() >= mMaxLen) {
        this.wait();
      }

      doEnqueue(t);
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean offer(T t) {
    synchronized (this) {
      if (mSize.get() >= mMaxLen) {
        return false; // Wouldn't fit.
      }

      doEnqueue(t);
    }

    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(T t) {
    synchronized (this) {
      int off = mDequeueOff;
      int size = mSize.get();
      for (int i = 0; i < size; i++) {
        T obj = (T) mArray[off];
        if (obj == null && t == null) {
          return true;
        } else if (obj != null && obj.equals(t)) {
          return true;
        }

        off++;
        if (off > mMaxLen) {
          off = 0;
        }
      }

      return false;
    }
  }
}
