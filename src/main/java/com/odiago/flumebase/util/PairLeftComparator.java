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

import java.util.Comparator;

import com.cloudera.util.Pair;

/**
 * Compare Pair objects based on the left element.
 */
public class PairLeftComparator<T extends Comparable<T>, U> implements Comparator<Pair<T, U>> {
  public int compare(Pair<T,U> o1, Pair<T,U> o2) {
    T first = o1.getLeft();
    T second = o2.getLeft();

    return first.compareTo(second);
  }
}
