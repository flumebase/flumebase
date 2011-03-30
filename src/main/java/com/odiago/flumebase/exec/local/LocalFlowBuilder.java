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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.BucketedAggregationElement;
import com.odiago.flumebase.exec.EvaluationElement;
import com.odiago.flumebase.exec.FileSourceElement;
import com.odiago.flumebase.exec.FlowElement;
import com.odiago.flumebase.exec.FlowElementContext;
import com.odiago.flumebase.exec.FlowId;
import com.odiago.flumebase.exec.FlumeNodeElement;
import com.odiago.flumebase.exec.HashJoinElement;
import com.odiago.flumebase.exec.InMemStreamSymbol;
import com.odiago.flumebase.exec.OutputElement;
import com.odiago.flumebase.exec.ProjectionElement;
import com.odiago.flumebase.exec.FilterElement;
import com.odiago.flumebase.exec.StreamSymbol;
import com.odiago.flumebase.exec.Symbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.flume.EmbeddedFlumeConfig;

import com.odiago.flumebase.parser.EntityTarget;
import com.odiago.flumebase.parser.Expr;

import com.odiago.flumebase.plan.AggregateNode;
import com.odiago.flumebase.plan.OutputNode;
import com.odiago.flumebase.plan.CreateStreamNode;
import com.odiago.flumebase.plan.DescribeNode;
import com.odiago.flumebase.plan.DropNode;
import com.odiago.flumebase.plan.EvaluateExprsNode;
import com.odiago.flumebase.plan.HashJoinNode;
import com.odiago.flumebase.plan.MemoryOutputNode;
import com.odiago.flumebase.plan.NamedSourceNode;
import com.odiago.flumebase.plan.PlanNode;
import com.odiago.flumebase.plan.ProjectionNode;
import com.odiago.flumebase.plan.FilterNode;

import com.odiago.flumebase.server.UserSession;

import com.odiago.flumebase.util.DAG;
import com.odiago.flumebase.util.DAGOperatorException;

/**
 * Traverse a FlowSpecification, building an actual flow based on the
 * specification nodes suitable for execution in the LocalEnvironment.  This
 * is intended to be run with the FlowSpecification.reverseBfs() method. 
 */
public class LocalFlowBuilder extends DAG.Operator<PlanNode> {
  private static final Logger LOG = LoggerFactory.getLogger(
      LocalFlowBuilder.class.getName());

  /**
   * Key in the PlanNode attribute map for the FlowElement associated
   * with the node.
   */
  private static final String LOCAL_FLOW_ELEM_KEY = "LocalFlowBuilder.flowElem";

  private FlowId mFlowId;
  private LocalFlow mLocalFlow;
  private SymbolTable mRootSymbolTable;
  private EmbeddedFlumeConfig mFlumeConfig;
  private Map<String, MemoryOutputElement> mMemOutputMap;
  private UserSession mSubmitterSession;

  public LocalFlowBuilder(FlowId flowId, SymbolTable rootSymTable,
      EmbeddedFlumeConfig flumeConfig, Map<String, MemoryOutputElement> memOutputMap,
      UserSession submitterSession) {
    mFlowId = flowId;
    mMemOutputMap = memOutputMap;
    mLocalFlow = new LocalFlow(flowId);
    mRootSymbolTable = rootSymTable;
    mFlumeConfig = flumeConfig;
    mSubmitterSession = submitterSession;
  }

  /**
   * Given a list of PlanNodes that have already been mapped internally
   * to FlowElementNodes, retrieve a list of FlowElementNodes they map to.
   */
  private List<FlowElementNode> getNodeElements(List<PlanNode> nodes) {
    List<FlowElementNode> out = new ArrayList<FlowElementNode>(nodes.size());
    for (PlanNode node : nodes) {
      FlowElementNode fen = (FlowElementNode) node.getAttr(LOCAL_FLOW_ELEM_KEY);
      assert null != fen;
      out.add(fen);
    }

    return out;
  }

  /**
   * @return true if the specified PlanNode will result in a multithreaded
   * FlowElement.
   */
  private boolean isMultiThreaded(PlanNode node, SymbolTable rootTable) {
    if (node instanceof NamedSourceNode) {
      return true;
    } else {
      // To date, all non-source nodes are all single-threaded.
      return false;
    }
  }

  /**
   * Given a PlanNode, produce the FlowElementContext that is appropriate
   * for connecting to all of its downstream components.
   */
  private FlowElementContext makeContextForNode(PlanNode node, SymbolTable rootTable) {
    List<PlanNode> children = node.getChildren();
    List<FlowElementNode> childElements = getNodeElements(children);
    boolean isMultiThreaded = isMultiThreaded(node, rootTable);
    if (childElements.size() == 0) {
      return new SinkFlowElemContext(mFlowId);
    } else if (childElements.size() == 1 &&
        (Boolean) node.getAttr(PlanNode.USES_TIMER_ATTR, Boolean.FALSE) == true) {
      // This node has only one 'official' output, but will instantiate a separate
      // FlowElement servicing interrupts from a timer thread. Use a normal connection
      // to the official output, but use this context to manage a queue into the timer
      // FlowElement as well.
      FlowElement childElem = childElements.get(0).getFlowElement();
      childElem.registerUpstream();
      return new TimerFlowElemContext(childElem);
    } else if (childElements.size() == 1 && !isMultiThreaded) {
      // Normal direct connection from node to node.
      FlowElement childElem = childElements.get(0).getFlowElement();
      childElem.registerUpstream();
      return new DirectCoupledFlowElemContext(childElem);
    } else if (childElements.size() == 1 && isMultiThreaded) {
      // We should put a buffer between ourselves and the child node.
      FlowElement childElem = childElements.get(0).getFlowElement();
      childElem.registerUpstream();
      return new MTGeneratorElemContext(childElem);
    } else {
      // TODO(aaron): Create a multi-output context and use here.
      LOG.error("No local context available for fan-out");
      return null;
    }
  }

  /**
   * Given a PlanNode and a new FlowElement node representing the flow component
   * for it, map the child links from this flowNode to the corresponding flowNodes
   * associated with the children of the planNode.
   */
  private void mapChildren(PlanNode planNode, FlowElementNode flowNode) {
    List<PlanNode> planChildren = planNode.getChildren();
    List<FlowElementNode> childFlowElems = getNodeElements(planChildren);
    for (FlowElementNode child : childFlowElems) {
      flowNode.addChild(child);
    }
  }

  public void process(PlanNode node) throws DAGOperatorException {
    FlowElement newElem = null; // The newly-constructed FlowElement.
    FlowElementContext newContext = makeContextForNode(node, mRootSymbolTable);

    if (null == node) {
      LOG.warn("Null node in plan graph");
      return;
    } else if (node instanceof OutputNode) {
      OutputNode outputNode = (OutputNode) node;
      String logicalFlumeNode = outputNode.getFlumeNodeName();
      newElem = new OutputElement(newContext,
          (Schema) outputNode.getAttr(PlanNode.INPUT_SCHEMA_ATTR),
          outputNode.getInputFields(), mFlumeConfig, logicalFlumeNode,
          (Schema) outputNode.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR),
          outputNode.getOutputFields(), mRootSymbolTable);
      if (null != logicalFlumeNode) {
        mLocalFlow.setFlumeRequired(true);
      }

    } else if (node instanceof MemoryOutputNode) {
      MemoryOutputNode memoryNode = (MemoryOutputNode) node;
      newElem = new MemoryOutputElement(newContext, memoryNode.getFields());
      String bufferName = memoryNode.getName();
      // Bind this buffer name to this memory node in the map provided
      // by the client.
      mMemOutputMap.put(bufferName, (MemoryOutputElement) newElem);
    } else if (node instanceof CreateStreamNode) {
      // Just perform this operation immediately. Do not translate this into another
      // layer. (This results in an empty flow being generated, which is discarded.)
      CreateStreamNode createStream = (CreateStreamNode) node;
      String streamName = createStream.getName();
      StreamSymbol streamSym = new StreamSymbol(createStream);
      if (!streamSym.getEventParser().validate(streamSym)) {
        // Fails final check of parameters
        // TODO: The EventParser is giving better info in its LOG; but this
        // should really be communicated back to the user.
        throw new DAGOperatorException(
            "Stream cannot be created with the specified parameters.");
      } else if (mRootSymbolTable.resolve(streamName) != null) {
        // TODO: Allow CREATE OR REPLACE STREAM to override this.
        throw new DAGOperatorException("Object already exists at top level: " + streamName);
      } else {
        mRootSymbolTable.addSymbol(streamSym);
        mSubmitterSession.sendInfo("CREATE STREAM");
      }
    } else if (node instanceof DescribeNode) {
      // Look up the referenced object in the symbol table and describe it immediately.
      DescribeNode describe = (DescribeNode) node;
      Symbol sym = mRootSymbolTable.resolve(describe.getIdentifier());
      mSubmitterSession.sendInfo(sym.toString());
    } else if (node instanceof DropNode) {
      // Perform the operation here.
      // Remove the objet from our symbol table.
      DropNode dropNode = (DropNode) node;
      String name = dropNode.getName();
      Symbol sym = mRootSymbolTable.resolve(name);
      if (null == sym) {
        // Shouldn't happen; the type checker already accepted this statement.
        throw new DAGOperatorException("No such object at top level: " + name);
      }
      EntityTarget targetType = dropNode.getType();
      // Perform the operation.
      mRootSymbolTable.remove(name);
      mSubmitterSession.sendInfo("DROP " + targetType.toString().toUpperCase());
    } else if (node instanceof NamedSourceNode) {
      NamedSourceNode namedInput = (NamedSourceNode) node;
      String streamName = namedInput.getStreamName();
      Symbol symbol = mRootSymbolTable.resolve(streamName).resolveAliases();
      if (null == symbol) {
        throw new DAGOperatorException("No symbol for stream: " + streamName);
      }

      if (!(symbol instanceof StreamSymbol)) {
        throw new DAGOperatorException("Identifier " + streamName + " has type: "
            + symbol.getType() + ", not STREAM.");
      }

      StreamSymbol streamSymbol = (StreamSymbol) symbol;

      switch (streamSymbol.getSourceType()) {
      case File:
        String fileName = streamSymbol.getSource();
        newElem = new FileSourceElement(newContext, fileName, streamSymbol.isLocal(),
            namedInput.getFields(), streamSymbol);
        break;
      case Source:
        if (!streamSymbol.isLocal()) {
          throw new DAGOperatorException("Do not know how to handle a non-local source yet.");
        }
        String flumeSource = streamSymbol.getSource();
        long flowIdNum = mFlowId.getId();
        String flowSourceId = "flumebase-flow-" + flowIdNum + "-" + streamSymbol.getName();
        newElem = new LocalFlumeSourceElement(newContext, flowSourceId,
            mFlumeConfig, flumeSource, (Schema) namedInput.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR),
            namedInput.getFields(), streamSymbol);
        if (!streamSymbol.isLocal()) {
          LOG.info("Created local Flume logical node: " + flowSourceId);
          LOG.info("You may need to connect upstream Flume elements to this source.");
        }

        // Mark Flume as required to execute this flow.
        mLocalFlow.setFlumeRequired(true);
        break;
      case Memory:
        newElem = new LocalInMemSourceElement(newContext,
            namedInput.getFields(), (InMemStreamSymbol) streamSymbol);
        break;
      case Node:
        String nodeSourceId = "flumebase-flow-" + mFlowId.getId() + "-" + streamSymbol.getName();
        newElem = new FlumeNodeElement(newContext, nodeSourceId,
            mFlumeConfig, streamSymbol.getSource(),
            (Schema) namedInput.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR),
            namedInput.getFields(), streamSymbol);

        LOG.info("Created local Flume receiver context: " + nodeSourceId);
        LOG.info("This will be connected to upstream Flume node: " + streamSymbol.getSource());

        // Mark Flume as required to execute this flow.
        mLocalFlow.setFlumeRequired(true);
        break;
      default:
        throw new DAGOperatorException("Unhandled stream source type: "
            + streamSymbol.getSourceType());
      }
    } else if (node instanceof FilterNode) {
      FilterNode filterNode = (FilterNode) node;
      Expr filterExpr = filterNode.getFilterExpr();
      newElem = new FilterElement(newContext, filterExpr);
    } else if (node instanceof ProjectionNode) {
      ProjectionNode projNode = (ProjectionNode) node;
      Schema outSchema = (Schema) projNode.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      newElem = new ProjectionElement(newContext, outSchema, projNode.getInputFields(),
          projNode.getOutputFields());
    } else if (node instanceof AggregateNode) {
      AggregateNode aggNode = (AggregateNode) node;
      newElem = new BucketedAggregationElement(newContext, aggNode);
    } else if (node instanceof EvaluateExprsNode) {
      EvaluateExprsNode evalNode = (EvaluateExprsNode) node;
      Schema outSchema = (Schema) evalNode.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      newElem = new EvaluationElement(newContext, evalNode.getExprs(),
          evalNode.getPropagateFields(), outSchema);
    } else if (node instanceof HashJoinNode) {
      HashJoinNode joinNode = (HashJoinNode) node;
      newElem = new HashJoinElement(newContext, joinNode);
    } else {
      throw new DAGOperatorException("Cannot create FlowElement for PlanNode of type: "
          + node.getClass().getName());
    }

    if (null != newElem) {
      // Wrap the FlowElement in a DAGNode. 
      FlowElementNode elemHolder = new FlowElementNode(newElem);
      mapChildren(node, elemHolder);
      elemHolder.setId(node.getId());

      // Bind the FlowElementNode to the PlanNode.
      node.setAttr(LOCAL_FLOW_ELEM_KEY, elemHolder);
      if (node.isRoot()) {
        // Roots of the plan node => this is a root node in the flow.
        mLocalFlow.addRoot(elemHolder);
      }

      // If we created a BucketedAggregationElement, create its timeout coprocessor.
      if (newElem instanceof BucketedAggregationElement) {
        BucketedAggregationElement bucketElem = (BucketedAggregationElement) newElem;

        FlowElement downstream = getNodeElements(node.getChildren()).get(0).getFlowElement();

        FlowElementContext timeoutContext = new DirectCoupledFlowElemContext(downstream);
        BucketedAggregationElement.TimeoutEvictionElement timeoutElem =
            bucketElem.getTimeoutElement(timeoutContext);
        // The timeout element is now upstream to the primary downstream element of the
        // BucketedAggregationElement.
        downstream.registerUpstream();
        timeoutElem.registerUpstream(); // BucketedAggEl't is upstream of the timeout elem.

        // Add the timeout element to the BucketedAggregationElement's output list.
        // Specify it as the timerElement, since this is a special designation in the
        // TimerFlowElemContext.
        ((TimerFlowElemContext) newContext).setTimerElement(timeoutElem);

        // Set up the control graph dependencies: the downstream (child) element(s) of the
        // bucketed aggregation element are also downstream of the timeout element.  The
        // timeout element itself is virtually downstream from the bucketed aggregation
        // element too.
        FlowElementNode timeoutHolder = new FlowElementNode(timeoutElem);
        for (FlowElementNode childNode : elemHolder.getChildren()) {
          timeoutHolder.addChild(childNode);
        }

        timeoutHolder.addParent(elemHolder);
      }
    }
  }

  /**
   * @return the LocalFlow containing the graph of FlowElements constructed in
   * the processing phase.
   */
  public LocalFlow getLocalFlow() {
    return mLocalFlow;
  }
}
