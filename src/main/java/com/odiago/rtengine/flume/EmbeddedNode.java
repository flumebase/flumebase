// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.io.IOException;

import org.apache.thrift.TException;

import com.odiago.rtengine.exec.FlowElementContext;

/**
 * Container that launches an embedded Flume physical node and hosts
 * a logical node on it, configured with the appropriate EventSink to
 * deliver data to a processing flow.
 */
public class EmbeddedNode {

  /**
   * Name of the flow and source we're providing data for; this is used as
   * the logical name of the embedded FlumeNode.
   */
  private String mFlowSourceId;

  /** FlowElementContext for the source FlowElement that wraps this EmbeddedNode. */
  private FlowElementContext mFlowElemContext;

  /** Manager for the logical Flume nodes in this process. */
  private EmbeddedFlumeConfig mFlumeConfig;

  /** Flume instruction for where to source the data from for this node. */
  private String mDataSource;

  /**
   * Create a single embedded node instance.
   * @param flowSourceId - the flowId and source name within the flow being fulfilled.
   * @param flowContext - the context for the source FlowElement wrapping this object.
   * @param flumeConfig - the manager of the embedded Flume instance.
   * @param dataSource - the Flume 'source' argument for the logical node.
   */
  public EmbeddedNode(String flowSourceId, FlowElementContext flowContext,
      EmbeddedFlumeConfig flumeConfig, String dataSource) {
    mFlowSourceId = flowSourceId;
    mFlowElemContext = flowContext;
    mFlumeConfig = flumeConfig;
    mDataSource = dataSource;
  }

  /**
   * Start the embedded node instance.
   */
  public void open() throws IOException {
    SinkContextBindings.get().bindContext(mFlowSourceId, mFlowElemContext);
    try {
      mFlumeConfig.createFlowSink(mFlowSourceId, mDataSource);
    } catch (TException te) {
      throw new IOException(te);
    }
  }

  /**
   * Stop the embedded node instance.
   */
  public void close() throws IOException {
    try {
      mFlumeConfig.stopFlowSink(mFlowSourceId);
    } catch (TException te) {
      throw new IOException(te);
    }
    SinkContextBindings.get().dropContext(mFlowSourceId);
  }
}
