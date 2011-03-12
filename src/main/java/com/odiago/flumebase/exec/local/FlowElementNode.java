// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec.local;

import com.odiago.flumebase.exec.FlowElement;

import com.odiago.flumebase.util.DAGNode;

/**
 * A DAGNode that holds a FlowElement in a local flow.
 */
public class FlowElementNode extends DAGNode<FlowElementNode> {
  private FlowElement mElem;

  public FlowElementNode(FlowElement fe) {
    super(0); // don't worry about node ids in this graph.
    mElem = fe;
  }

  public FlowElement getFlowElement() {
    return mElem;
  }

  @Override
  protected void formatParams(StringBuilder sb) {
    sb.append(mElem.toString());
  }

}
