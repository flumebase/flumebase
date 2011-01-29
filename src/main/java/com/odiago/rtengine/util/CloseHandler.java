// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util;

/**
 * Interface that designates that an object should be notified when some
 * other object is closed. The other object should maintain a set of
 * subscribers who are notified when it is closed, and call their
 * handleClose() methods. @see ClosePublisher for an implementation of
 * this service.
 */
public interface CloseHandler<T> {
  void handleClose(T instance);
}
