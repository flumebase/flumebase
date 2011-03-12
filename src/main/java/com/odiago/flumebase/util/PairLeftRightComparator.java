// (c) Copyright 2011 Odiago, Inc.

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
