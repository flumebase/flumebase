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

package com.odiago.flumebase.exec.local;

import java.io.IOException;

import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.FlowElementImpl;
import com.odiago.flumebase.exec.StreamSymbol;

import com.odiago.flumebase.flume.EmbeddedFlumeConfig;
import com.odiago.flumebase.flume.EmbeddedNode;

import com.odiago.flumebase.parser.TypedField;

/**
 * FlowElement providing source data from a local Flume source.
 */
public class LocalFlumeSourceElement extends FlowElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalFlumeSourceElement.class.getName());

  /** Flume's name for this logical node. */
  private String mFlowSourceId;

  /** The manager of the embedded Flume node instances. */
  private EmbeddedFlumeConfig mFlumeConfig;

  /** The Flume logical node resources associated with this FlowElement. */
  private EmbeddedNode mEmbeddedFlumeNode;

  /**
   * The Avro record schema for event bodies emitted by this node into our
   * internal pipeline.
   */
  private Schema mOutputSchema;

  /** The fields of each record emitted by this node, and their types. */ 
  private List<TypedField> mFieldTypes;

  /** Symbol for the stream we are reading from. */
  private StreamSymbol mStreamSym;

  /**
   * The EventSource that we couple to our internal sink for
   * this logical node.
   */
  private String mDataSource;

  public LocalFlumeSourceElement(FlowElementContext context, String flowSourceId,
      EmbeddedFlumeConfig flumeConfig, String dataSource, Schema outputSchema,
      List<TypedField> fieldTypes, StreamSymbol streamSym) {
    super(context);

    mFlowSourceId = flowSourceId;
    mFlumeConfig = flumeConfig;
    mDataSource = dataSource;
    mOutputSchema = outputSchema;
    mFieldTypes = fieldTypes;
    mStreamSym = streamSym;
  }

  @Override
  public void open() throws IOException, InterruptedException {
    super.open();
    mEmbeddedFlumeNode = new EmbeddedNode(mFlowSourceId, getContext(), mFlumeConfig,
        mDataSource, mOutputSchema, mFieldTypes, mStreamSym);
    mEmbeddedFlumeNode.open();
  }

  @Override
  public void close() throws IOException, InterruptedException {
    mEmbeddedFlumeNode.close();
    super.close();
  }

  @Override
  public void takeEvent(EventWrapper e) {
    // We generate our own events; nothing should be upstream from us.
    throw new RuntimeException("LocalFlumeSinkElement does not support incoming events");
  }

  @Override
  public String toString() {
    return "FlumeSource[mFlowSourceId=\"" + mFlowSourceId + "\", "
        + "mDataSource=\"" + mDataSource + "\"]";
  }
}
