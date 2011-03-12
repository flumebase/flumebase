// (c) Copyright 2011 Odiago, Inc.

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
