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

import java.util.Iterator;

/**
 * Wrapper that turns an Iterator&lt;T&gt; into an Iterable&lt;T&gt;.
 */
public class IterableIterator<T> implements Iterable<T> {
  private Iterator<T> mIterator;

  public IterableIterator(Iterator<T> iter) {
    mIterator = iter;
  }

  public Iterator<T> iterator() {
    return mIterator;
  }
}
