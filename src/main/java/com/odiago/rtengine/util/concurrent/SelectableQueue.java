// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

/**
 * Queue that implements the Selectable interface.
 */
public abstract class SelectableQueue<T> extends Selectable<T> {
  /** Return the next object in the queue. Will block if the
   * queue does not contain any items.
   */
  public abstract T take() throws InterruptedException;

  /**
   * Returns the next object in the queue without waiting.  If the queue does
   * not contain any items, throws EmptyException.
   */
  public abstract T poll() throws EmptyException;

  /**
   * Adds an item to the back of the queue. Implementations of
   * SelectableQueue with bounded storage may block if space
   * is not available.
   */
  public abstract void put(T t) throws InterruptedException;

  /**
   * Adds an item to the back of the queue, if it can be inserted
   * without waiting.
   */
  public abstract boolean offer(T t);

  /** @return true if the queue contains an element x such that x.equals(t). */
  public abstract boolean contains(T t);

  /**
   * Returns the length of the queue.
   */
  public abstract int size(); 

  @Override
  public boolean canRead() {
    return size() > 0;
  }

  /**
   * Dequeues the next item from the queue with dequeue() and returns the
   * result. */
  @Override
  public T read() throws InterruptedException {
    return take();
  }
}
