// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Node that emits all input records to the console.
 */
public class ConsoleOutputNode extends PlanNode {
  @Override 
  public void formatParams(StringBuilder sb) {
    sb.append("output to console");
  }
}
