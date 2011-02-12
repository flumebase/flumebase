// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

import java.util.ArrayList;
import java.util.List;

/**
 * A shared object which supports a blocking read operation. Each
 * read operation returns a value of type T.
 */
public abstract class Selectable<T> {

  /** Select instances to notify when we're ready for read. */
  private List<Select<T>> mSelects;

  public Selectable() {
    mSelects = new ArrayList<Select<T>>();
  }

  /**
   * Specifies which Select instance(s) should be notified when the object
   * is ready. Called by the Select instance, not by users.
   */
  void register(Select<T> sel) {
    synchronized (mSelects) {
      mSelects.add(sel);
    }
  }

  /**
   * Specifies a Select instance that should no longer be notified when
   * the object is ready for read. Called by the Select instance, not by users.
   */
  void unregister(Select<T> sel) {
    synchronized (mSelects) {
      mSelects.remove(sel);
    }
  }

  /**
   * Method that should be called by any producer methods when they are
   * complete, to indicate to any affiliated Select instances that
   * they can try to read now.
   */
  protected void notifyReaders() {
    synchronized (mSelects) {
      for (Select<T> select : mSelects) {
        synchronized (select) {
          select.enqueueSelectable(this);
        }
      }
    }
  }

  /**
   * Must return true if the current state of the object would support
   * a read operation. This method may synchronize on the object itself.
   * Callers are encouraged to synchronize on the instance before calling
   * canRead() so that its output can be meaningfully combined with a
   * read operation.
   */
  public abstract boolean canRead();

  /**
   * Reads from the object. Blocks until canRead() would return true,
   * then reads the datum and returns it as an object of type T.
   */
  public abstract T read() throws InterruptedException;
}
