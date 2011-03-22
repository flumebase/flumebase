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
 * Compare Pair objects based on the left element first, breaking ties by
 * comparing based on the right element.
 */
public class PairLeftRightComparator<T extends Comparable<T>, U extends Comparable<U>>
    implements Comparator<Pair<T, U>> {

  public int compare(Pair<T,U> o1, Pair<T,U> o2) {
    T fstLeft = o1.getLeft();
    T sndLeft = o2.getLeft();

    int left = fstLeft.compareTo(sndLeft);
    if (0 == left) {
      U fstRight = o1.getRight();
      U sndRight = o2.getRight();
      return fstRight.compareTo(sndRight);
    } else {
      return left;
    }
  }
}
