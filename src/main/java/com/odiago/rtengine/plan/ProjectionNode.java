// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Node that projects from an input schema onto an output schema.
 * This node has no explicit parameters; the input/output schemas are defined
 * as flow attributes which are set externally and propagated by a forward
 * pass.
 */
public class ProjectionNode extends PlanNode {
  public ProjectionNode() {
  }
}
