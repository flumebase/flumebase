// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.flume;

import java.io.IOException;

import java.util.List;

import org.apache.avro.Schema;

import org.apache.thrift.TException;

import com.odiago.rtengine.exec.FlowElementContext;

import com.odiago.rtengine.parser.TypedField;

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

  /** Schema for records emitted by this node. */
  private Schema mOutputSchema;

  /** List of fields and types emitted by this node. */
  private List<TypedField> mFieldTypes;

  /** Name of the stream we represent. */
  private String mStreamName;

  /**
   * Create a single embedded node instance.
   * @param flowSourceId - the flowId and source name within the flow being fulfilled.
   * @param flowContext - the context for the source FlowElement wrapping this object.
   * @param flumeConfig - the manager of the embedded Flume instance.
   * @param dataSource - the Flume 'source' argument for the logical node.
   * @param streamName - the name of the stream we are reading from into the query.
   */
  public EmbeddedNode(String flowSourceId, FlowElementContext flowContext,
      EmbeddedFlumeConfig flumeConfig, String dataSource, Schema outputSchema,
      List<TypedField> fieldTypes, String streamName) {
    mFlowSourceId = flowSourceId;
    mFlowElemContext = flowContext;
    mFlumeConfig = flumeConfig;
    mDataSource = dataSource;
    mOutputSchema = outputSchema;
    mFieldTypes = fieldTypes;
    mStreamName = streamName;
  }

  /**
   * Start the embedded node instance.
   */
  public void open() throws IOException {
    SinkContextBindings.get().bindContext(mFlowSourceId,
        new SinkContext(mFlowElemContext, mOutputSchema, mFieldTypes, mStreamName));
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
