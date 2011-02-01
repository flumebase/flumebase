// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

/**
 * SelectableQueue implementation that is internally
 * synchronized. Calling any of its public operations
 * is permitted from any thread without further synchronization.
 * The caller may optionally synchronize on the queue before
 * calling any operations.
 *
 * <p>This queue is unbounded in length. The enqueue()
 * operation will not block.</p>
 */
public class SyncSelectableQueue<T> extends SelectableQueue<T> {
  
  /** Item in the queue. */
  private static class Item<T> {
    private final T mVal;
    private Item<T> mNext;

    public Item(T val, Item<T> next) {
      mVal = val;
      mNext = next;
    }

    public T val() {
      return mVal;
    }

    public Item<T> next() {
      return mNext;
    }

    public void setNext(Item<T> next) {
      mNext = next;
    }

  }

  private Item<T> mHead; // Where objects dequeue from.
  private Item<T> mTail; // Where objects enqueue to.
  private int mLength;

  /** Return the next object in the queue. Will block if the
   * queue does not contain any items.
   */
  @Override
  public T take() throws InterruptedException {
    synchronized (this) {
      while (mHead == null) {
        this.wait();
      }

      assert mHead != null;

      Item<T> next = mHead.next();
      T retval = mHead.val();
      if (mHead == mTail) {
        mTail = null;
      }
      mHead = next;
      mLength--;

      return retval;
    }
  }

  /**
   * Returns the next object in the queue. If the queue does not
   * contain any items, throws EmptyException.
   */
  @Override
  public T poll() throws EmptyException {
    synchronized (this) {
      if (null == mHead) {
        throw new EmptyException();
      }

      Item<T> next = mHead.next();
      T retval = mHead.val();
      if (mHead == mTail) {
        mTail = null;
      }
      mHead = next;
      mLength--;

      return retval;
    }
  }

  /**
   * Adds an item to the back of the queue. This version does
   * not block.
   */
  @Override
  public void put(T t) throws InterruptedException {
    synchronized (this) {
      Item<T> newItem = new Item<T>(t, null);
      if (null == mHead) {
        mHead = newItem;
      }
      if (null != mTail) {
        mTail.setNext(newItem);
      }
      mTail = newItem;
      mLength++;
      this.notify();
      notifyReaders();
    }
  }

  @Override
  public boolean offer(T t) throws InterruptedException {
    put(t);
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean contains(T t) {
    Item<T> cur = mHead;

    while (null != cur) {
      T val = cur.val();
      if (val == null && t == null) {
        return true;
      } else if (val != null && val.equals(t)) {
        return true;
      }
      cur = cur.next();
    }

    return false;
  }

  /** {@inheritDoc} */
  @Override
  public int size() {
    synchronized (this) {
      return mLength;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean canRead() {
    synchronized (this) {
      return mHead != null;
    }
  }
}
