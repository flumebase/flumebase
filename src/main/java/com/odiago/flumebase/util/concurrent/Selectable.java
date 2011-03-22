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

import java.util.ArrayList;
import java.util.List;

/**
 * A shared object which supports a blocking read operation. Each
 * read operation returns a value of type T.
 */
public abstract class Selectable<T> {

  /** Select instances to notify when we're ready for read. */
  private List<Select<T>> mSelects;

  public Selectable() {
    mSelects = new ArrayList<Select<T>>();
  }

  /**
   * Specifies which Select instance(s) should be notified when the object
   * is ready. Called by the Select instance, not by users.
   */
  void register(Select<T> sel) {
    synchronized (mSelects) {
      mSelects.add(sel);
    }
  }

  /**
   * Specifies a Select instance that should no longer be notified when
   * the object is ready for read. Called by the Select instance, not by users.
   */
  void unregister(Select<T> sel) {
    synchronized (mSelects) {
      mSelects.remove(sel);
    }
  }

  /**
   * Method that should be called by any producer methods when they are
   * complete, to indicate to any affiliated Select instances that
   * they can try to read now.
   */
  protected void notifyReaders() {
    synchronized (mSelects) {
      for (Select<T> select : mSelects) {
        synchronized (select) {
          select.enqueueSelectable(this);
        }
      }
    }
  }

  /**
   * Must return true if the current state of the object would support
   * a read operation. This method may synchronize on the object itself.
   * Callers are encouraged to synchronize on the instance before calling
   * canRead() so that its output can be meaningfully combined with a
   * read operation.
   */
  public abstract boolean canRead();

  /**
   * Reads from the object. Blocks until canRead() would return true,
   * then reads the datum and returns it as an object of type T.
   */
  public abstract T read() throws InterruptedException;
}
