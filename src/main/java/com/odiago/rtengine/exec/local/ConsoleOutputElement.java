// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import com.cloudera.flume.core.Event;

import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowElementImpl;

/**
 * FlowElement that prints events to the console.
 */
public class ConsoleOutputElement extends FlowElementImpl {
  public ConsoleOutputElement(FlowElementContext context) {
    super(context);
  }

  @Override
  public void takeEvent(Event e) {
    StringBuilder sb = new StringBuilder();
    long ts = e.getTimestamp();
    sb.append(ts);
    sb.append('\t');
    sb.append(new String(e.getBody()));
    System.out.println(sb.toString());
  }
}
