// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.util.HashMap;
import java.util.Map;

import com.odiago.rtengine.exec.FlowElementContext;

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
  private static SinkContextBindings mBindings;

  /** The actual mapping of names to FlowElementContexts. */
  private Map<String, FlowElementContext> mContextMap;

  private SinkContextBindings() {
    mContextMap = new HashMap<String, FlowElementContext>();
  }

  public static SinkContextBindings get() {
    if (null == mBindings) {
      mBindings = new SinkContextBindings();
    }

    return mBindings;
  }

  /**
   * Look up and return the FlowElementContext associated with a string id.
   * @returns the FlowElementContext for name, or null if name is not mapped.
   */
  public FlowElementContext getContext(String name) {
    return mContextMap.get(name);
  }

  /**
   * Bind a name to a specific FlowElementContext for later retrieval.
   */
  public void bindContext(String name, FlowElementContext context) {
    mContextMap.put(name, context);
  }

  /**
   * Remove a binding (e.g., because a flow is canceled).
   */
  public void dropContext(String name) {
    mContextMap.remove(name);
  }
}
