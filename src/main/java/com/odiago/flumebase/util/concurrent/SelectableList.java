// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public abstract class SelectableList<T> extends Selectable<T> implements List<T> {
  public abstract int size();
  public abstract boolean isEmpty();
  public abstract boolean contains(Object o);
  public abstract Iterator<T> iterator();
  public abstract Object[] toArray();
  public abstract boolean add(T e);
  public abstract boolean remove(Object o);
  public abstract boolean containsAll(Collection<?> c);
  public abstract boolean addAll(Collection<? extends T> c);
  public abstract boolean addAll(int index, Collection<? extends T> c);
  public abstract boolean removeAll(Collection<?> c);
  public abstract boolean retainAll(Collection<?> c);
  public abstract void clear();
  public abstract boolean equals(Object o);
  public abstract int hashCode();
  public abstract T get(int index);
  public abstract T set(int index, T element);
  public abstract void add(int index, T element);
  public abstract T remove(int index);
  public abstract int indexOf(Object o);
  public abstract int lastIndexOf(Object o);
  public abstract ListIterator<T> listIterator();
  public abstract ListIterator<T> listIterator(int index);
  public abstract List<T> subList(int fromIndex, int toIndex);

  public boolean canRead() {
    return size() > 0;
  }

  public abstract T read() throws InterruptedException;
}
