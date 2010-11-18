// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

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

  public int size() {
    return mStore.size();
  }
}
