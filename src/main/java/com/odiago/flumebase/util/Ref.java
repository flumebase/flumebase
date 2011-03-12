// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.util;

/**
 * Generic indirection box.
 */
public class Ref<T> {
  public T item;

  public Ref() {
    this(null);
  }

  public Ref(T obj) {
    item = obj;
  }

  @Override
  public int hashCode() {
    return item == null ? 0 : item.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    return (((Ref) other).item == null && item == null)
        || (item != null && item.equals(((Ref) other).item));
  }

  @Override
  public String toString() {
    return "" + item;
  }
}
