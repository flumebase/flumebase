// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

/**
 * Context for a FlowElement which is itself a sink; it cannot emit data
 * to any downstream elements, for there are none.
 */
public class SinkFlowElemContext extends FlowElementContext {

  public SinkFlowElemContext() {
  }

  @Override
  public void emit(Event e) throws IOException, InterruptedException {
    throw new IOException("Cannot emit event without downstream element");
  }
}
