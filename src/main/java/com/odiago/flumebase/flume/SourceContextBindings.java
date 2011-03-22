/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.odiago.flumebase.flume;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapping from names associated with flows to contexts associated with
 * EventSources that live at the endpoints, broadcasting results to Flume.
 *
 * This mapping is used when Flume EventSources are opened by flow
 * OutputElements, to attach a queue to deliver the data from the flow. Since
 * the EventSource needs to access this from a global context, this is
 * implemented as a singleton.
 */
public final class SourceContextBindings {
  /** The singleton instance of this. */
  private static final SourceContextBindings mBindings;
  static {
    mBindings = new SourceContextBindings();
  }


  /** The actual mapping of names to SourceContexts. */
  private Map<String, SourceContext> mContextMap;

  private SourceContextBindings() {
    mContextMap = Collections.synchronizedMap(new HashMap<String, SourceContext>());
  }

  public static SourceContextBindings get() {
    return mBindings;
  }

  /**
   * Look up and return the SourceContext associated with a string id.
   * @returns the SourceContext for name, or null if name is not mapped.
   */
  public SourceContext getContext(String name) {
    return mContextMap.get(name);
  }

  /**
   * Bind a name to a specific SourceContext for later retrieval.
   */
  public void bindContext(String name, SourceContext context) {
    mContextMap.put(name, context);
  }

  /**
   * Remove a binding (e.g., because a flow is canceled).
   */
  public void dropContext(String name) {
    mContextMap.remove(name);
  }
}
