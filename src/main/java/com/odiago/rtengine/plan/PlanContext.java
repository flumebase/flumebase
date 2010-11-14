// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

/**
 * Container for state associated with the plan-formation process
 * when operating over the statement AST.
 */
public class PlanContext {
  /** The string builder for messages to the user generated during planning. */
  private StringBuilder mMsgBuilder;

  /** The DAG we are forming to plan this query. */
  private FlowSpecification mFlowSpec;

  /**
   * True if we are building the 'root' FlowSpecification; false if we are
   * building a FlowSpecification intended to be incorporated into a larger
   * FlowSpecification higher up in the AST.
   */
  private boolean mIsRoot;

  public PlanContext() {
    mMsgBuilder = new StringBuilder();
    mFlowSpec = new FlowSpecification();
    mIsRoot = true;
  }

  public PlanContext(StringBuilder msgBuilder, FlowSpecification flowSpec, boolean isRoot) {
    mMsgBuilder = msgBuilder;
    mFlowSpec = flowSpec;
    mIsRoot = isRoot;
  }

  public boolean isRoot() {
    return mIsRoot;
  }

  public void setRoot(boolean root) {
    mIsRoot = root;
  }

  public StringBuilder getMsgBuilder() {
    return mMsgBuilder;
  }

  public FlowSpecification getFlowSpec() {
    return mFlowSpec;
  }

  public void setMsgBuilder(StringBuilder sb) {
    mMsgBuilder = sb;
  }

  public void setFlowSpec(FlowSpecification flow) {
    mFlowSpec = flow;
  }
}
