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

public class Stack<E> {

  private ArrayList<E> mStore;

  public Stack() {
    mStore = new ArrayList<E>();
  }

  public void push(E obj) {
    mStore.add(obj);
  }

  public E pop() {
    return mStore.remove(mStore.size() - 1);
  }

  public E top() {
    return mStore.get(mStore.size() - 1);
  }

  public boolean isEmpty() {
    return mStore.isEmpty();
  }

  /**
   * @return the current height of the stack.
   */
  public int size() {
    return mStore.size();
  }

  /**
   * Set the stack to contain only 'k' items.
   */
  public void reset(int k) {
    while (size() > k) {
      pop();
    }
  }
}
