// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * ArrayList implementation that is selectable; other threads will be notified
 * when a user modifies the list.
 */
public class SyncSelectableList<T> extends SelectableList<T> {
  private ArrayList<T> mElements;

  public SyncSelectableList() {
    mElements = new ArrayList<T>();
  }

  public synchronized int size() {
    return mElements.size();
  }

  public synchronized boolean isEmpty() {
    return size() == 0;
  }

  public synchronized boolean contains(Object o) {
    return mElements.contains(o);
  }

  /**
   * Iterate over the elements. Clients should externally synchronize on this
   * object.
   */
  public Iterator<T> iterator() {
    return mElements.iterator();
  }

  public synchronized T[] toArray(Object[] in) {
    return mElements.toArray((T[]) in);
  }

  public synchronized Object[] toArray() {
    return mElements.toArray();
  }

  public synchronized boolean add(T e) {
    boolean out = mElements.add(e);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized boolean remove(Object o) {
    boolean out = mElements.remove(o);
    this.notify();
    notifyReaders();
    return out;
  }

  public synchronized boolean containsAll(Collection<?> c) {
    return mElements.containsAll(c);
  }

  public synchronized boolean addAll(Collection<? extends T> c) {
    boolean out = mElements.addAll(c);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized boolean addAll(int index, Collection<? extends T> c) {
    boolean out = mElements.addAll(index, c);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized boolean removeAll(Collection<?> c) {
    boolean out = mElements.removeAll(c);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized boolean retainAll(Collection<?> c) {
    boolean out = mElements.retainAll(c);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized void clear() {
    mElements.clear();
    notify();
    notifyReaders();
  }

  public synchronized boolean equals(Object o) {
    if (null == o) {
      return false;
    } else if (!getClass().equals(o.getClass())) {
      return false;
    }

    SyncSelectableList<T> other = (SyncSelectableList<T>) o;
    return mElements.equals(other.mElements);
  }

  public synchronized int hashCode() {
    return mElements.hashCode();
  }

  public synchronized T get(int index) {
    return mElements.get(index);
  }

  public synchronized T set(int index, T element) {
    T out = mElements.set(index, element);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized void add(int index, T element) {
    mElements.add(index, element);
    notify();
    notifyReaders();
  }

  public synchronized T remove(int index) {
    T out = mElements.remove(index);
    notify();
    notifyReaders();
    return out;
  }

  public synchronized int indexOf(Object o) {
    return mElements.indexOf(o);
  }

  public synchronized int lastIndexOf(Object o) {
    return mElements.lastIndexOf(o);
  }

  /**
   * Clients should externally synchronize on this object before using the iterator.
   */
  public ListIterator<T> listIterator() {
    return mElements.listIterator();
  }

  public ListIterator<T> listIterator(int index) {
    return mElements.listIterator(index);
  }

  public synchronized List<T> subList(int fromIndex, int toIndex) {
    return mElements.subList(fromIndex, toIndex);
  }

  /**
   * Returns the element at the end of the list.
   * Blocks if the list is empty.
   */
  public T read() throws InterruptedException {
    synchronized (this) {
      while (!canRead()) {
        wait();
      }

      return get(size() - 1);
    }
  }
}
