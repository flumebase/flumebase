// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

/**
 * An exception signalling that the current process should quit gracefully.
 */
public class QuitException extends Exception {
  private int mStatus;

  /**
   * Initialize a QuitException with the specified exit status code.
   */
  public QuitException(int code) {
    super("QuitException");
    mStatus = code;
  }

  /**
   * @return the exit status code for this quit operation.
   */
  public int getStatus() {
    return mStatus;
  }
}
