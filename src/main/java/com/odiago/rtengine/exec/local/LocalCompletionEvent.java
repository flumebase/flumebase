// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

/**
 * Indicates that a FlowElement in a LocalFlow has completed its processing,
 * and the LocalEnvironment should remove it from service to free resources;
 * downstream element(s) should be notified.
 */
public class LocalCompletionEvent {

  /** The completed FE's context. */
  private final LocalContext mCompleteContext;

  public LocalCompletionEvent(LocalContext context) {
    mCompleteContext = context;
  }

  public LocalContext getContext() {
    return mCompleteContext;
  }
}

