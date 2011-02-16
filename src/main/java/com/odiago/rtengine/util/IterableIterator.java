// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util;

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
