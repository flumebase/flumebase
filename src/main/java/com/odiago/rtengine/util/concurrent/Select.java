// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Select object can wait on multiple objects, returning when one of them
 * is ready for a read operation. Objects registered with a Select must
 * implement Selectable. 
 *
 * <p>The add() method should be repeatedly called to register one or more Selectable
 * objects with the Select. This will allow the Select to accept notifications
 * from the Selectables.</p>
 *
 * <p>add() and remove() are NOT thread-safe with respect to read() and join(),
 * although they are thread-safe with respect to each other. read() and join()
 * are also thread safe with respect to one another.</p>
 *
 * <p>Basically, this is designed for a multi-producer, single-consumer use
 * case. While you may use a single Select instance for the multi-consumer case,
 * you should not add() or remove() Selectables if another thread may call
 * read() or join() simultaneously.</p>
 *
 */
public class Select<T> implements Iterable<Selectable<T>> {

  /** The objects we are monitoring for readability. */
  private List<Selectable<T>> mTargets;

  public Select() {
    mTargets = new ArrayList<Selectable<T>>();
  }

  /**
   * Add an object to the list of objects to watch.
   */
  public void add(Selectable<T> target) {
    synchronized (target) {
      synchronized (this) {
        mTargets.add(target);
        target.register(this);
      }
    }
  }

  /**
   * Remove an object from the list of objects to watch.
   */
  public void remove(Selectable<T> target) {
    synchronized (target) {
      synchronized (this) {
        mTargets.remove(target);
        target.unregister(this);
      }
    }
  }

  /**
   * Get the set of objects being watched.
   */
  public Iterator<Selectable<T>> iterator() {
    return mTargets.iterator();
  }

  /**
   * Wait for one of the targets to become readable.  Returns a target that
   * MAY be readable. Clients should synchronize on the return value of this
   * and check whether it is readable, then read it. Clients should NOT
   * synchronize on the return value of this method within a code block
   * synchronized on this object itself. This method may return null if
   * multiple clients are using this Select instance. In this case, call
   * join() again.
   *
   * <p>For best results, use the read() method which ensures that proper
   * lock orderings are respected. Do not externally synchronize on this
   * Select instance.</p>
   */
  public Selectable<T> join() throws InterruptedException {
    // Try to pick one immediately.
    Selectable<T> ret = nextReady();
    if (null != ret) {
      return ret;
    }
    
    // Couldn't find a ready target. Wait for one to become available.
    do {
      synchronized (this) {
        this.wait();
      }
      ret = nextReady();
    } while (ret == null);

    return ret;
  }

  /**
   * Reads from the next available target, blocking until one is ready.
   */
  public T read() throws InterruptedException {
    while (true) {
      Selectable<T> target = join();

      assert target != null;

      synchronized (target) {
        if (target.canRead()) {
          return target.read();
        }
      }
    }
  }

  /**
   * @return the next ready target Selectable, or null if none is ready.
   */
  private Selectable<T> nextReady() {
    if (mTargets.size() == 0) {
      throw new RuntimeException("No targets registered with this Select instance.");
    }

    for (Selectable<T> target : mTargets) {
      if (target.canRead()) {
        return target;
      }
    }
    return null;
  }

  /** Called by Selectable when an object is readable. */
  void enqueueSelectable(Selectable<T> sel) throws InterruptedException {
    synchronized (this) {
      this.notify();
    }
  }
}
