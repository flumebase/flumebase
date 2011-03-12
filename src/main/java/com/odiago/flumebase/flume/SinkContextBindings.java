// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.flume;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapping from names associated with flows and their sources, to FlowElementContexts
 * used to transfer data downstream within a flow.
 *
 * This mapping is used when Flume EventSinks are opened by flow sources, to attach
 * a FlowElementContext to deliver the data into the flow. Since the EventSink
 * needs to access this from a global context, this is implemented as a singleton.
 */
public final class SinkContextBindings {
  /** The singleton instance of this. */
  private static final SinkContextBindings mBindings;
  static {
    mBindings = new SinkContextBindings();
  }

  /** The actual mapping of names to SinkContexts. */
  private Map<String, SinkContext> mContextMap;

  private SinkContextBindings() {
    mContextMap = Collections.synchronizedMap(new HashMap<String, SinkContext>());
  }

  public static SinkContextBindings get() {
    return mBindings;
  }

  /**
   * Look up and return the SinkContext associated with a string id.
   * @returns the SinkContext for name, or null if name is not mapped.
   */
  public SinkContext getContext(String name) {
    return mContextMap.get(name);
  }

  /**
   * Bind a name to a specific SinkContext for later retrieval.
   */
  public void bindContext(String name, SinkContext context) {
    mContextMap.put(name, context);
  }

  /**
   * Remove a binding (e.g., because a flow is canceled).
   */
  public void dropContext(String name) {
    mContextMap.remove(name);
  }
}
