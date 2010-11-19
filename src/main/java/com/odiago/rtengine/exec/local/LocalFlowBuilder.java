// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.StrMatchFilterElement;
import com.odiago.rtengine.exec.StreamSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.flume.EmbeddedFlumeConfig;

import com.odiago.rtengine.parser.StreamSourceType;

import com.odiago.rtengine.plan.ConsoleOutputNode;
import com.odiago.rtengine.plan.CreateStreamNode;
import com.odiago.rtengine.plan.DescribeNode;
import com.odiago.rtengine.plan.NamedSourceNode;
import com.odiago.rtengine.plan.PlanNode;
import com.odiago.rtengine.plan.StrMatchFilterNode;

import com.odiago.rtengine.util.DAG;

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

  public LocalFlowBuilder(FlowId flowId, SymbolTable rootSymTable,
      EmbeddedFlumeConfig flumeConfig) {
    mFlowId = flowId;
    mLocalFlow = new LocalFlow(flowId);
    mRootSymbolTable = rootSymTable;
    mFlumeConfig = flumeConfig;
  }

  /**
   * Given a list of PlanNodes that have already been mapped internally
   * to FlowElementNodes, retrieve a list of FlowElementNodes they map to.
   */
  private List<FlowElementNode> getNodeElements(List<PlanNode> nodes) {
    List<FlowElementNode> out = new ArrayList<FlowElementNode>(nodes.size());
    for (PlanNode node : nodes) {
      FlowElementNode fen = (FlowElementNode) node.getAttr(LOCAL_FLOW_ELEM_KEY);
      out.add(fen);
    }

    return out;
  }

  /**
   * Given a PlanNode, produce the FlowElementContext that is appropriate
   * for connecting to all of its downstream components.
   */
  private FlowElementContext makeContextForNode(PlanNode node) {
    List<PlanNode> children = node.getChildren();
    List<FlowElementNode> childElements = getNodeElements(children);
    if (childElements.size() == 0) {
      return new SinkFlowElemContext();
    } else if (childElements.size() == 1) {
      return new DirectCoupledFlowElemContext(childElements.get(0).getFlowElement());
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

  public void process(PlanNode node) {
    // TODO(aaron): Rework this to use a pattern such that the inability to
    // handle a certain type of PlanNode is actually a compile-time error.
    FlowElement newElem = null; // The newly-constructed FlowElement.
    FlowElementContext newContext = makeContextForNode(node);

    if (null == node) {
      LOG.warn("Null node in plan graph");
      return;
    } else if (node instanceof ConsoleOutputNode) {
      newElem = new ConsoleOutputElement(newContext);
    } else if (node instanceof CreateStreamNode) {
      // Just perform this operation immediately. Do not translate this into another
      // layer. (This results in an empty flow being generated, which is discarded.)
      CreateStreamNode createStream = (CreateStreamNode) node;
      String streamName = createStream.getName();
      StreamSymbol streamSym = new StreamSymbol(createStream);
      if (mRootSymbolTable.resolve(streamName) != null) {
        // TODO: Allow CREATE OR REPLACE STREAM to override this.
        System.err.println("Object already exists at top level: " + streamName);
      } else {
        mRootSymbolTable.addSymbol(streamSym);
        System.out.println("CREATE STREAM");
      }
    } else if (node instanceof DescribeNode) {
      // Look up the referenced object in the symbol table and describe it immediately.
      DescribeNode describe = (DescribeNode) node;
      Symbol sym = mRootSymbolTable.resolve(describe.getIdentifier());
      System.out.println(sym);
    } else if (node instanceof NamedSourceNode) {
      NamedSourceNode fileInput = (NamedSourceNode) node;
      String streamName = fileInput.getStreamName();
      Symbol symbol = mRootSymbolTable.resolve(streamName);
      if (null == symbol) {
        // TODO: Allow throwing an exception here. This needs to fail completely.
        LOG.error("No symbol for stream: " + streamName);
        return;
      }

      // TODO: Be paranoid about typechecking, make sure this is actually legal
      // first; throw an exception if not and cancel flow production.
      StreamSymbol streamSymbol = (StreamSymbol) symbol;
      if (!streamSymbol.isLocal()) {
        // TODO(aaron): BEtter exception for this.
        throw new RuntimeException("Do not know how to handle a non-local source yet.");
      }

      switch (streamSymbol.getSourceType()) {
      case File:
        String fileName = streamSymbol.getSource();
        newElem = new LocalFileSourceElement(newContext, fileName);
        break;
      case Sink:
        String flumeSource = streamSymbol.getSource();
        long flowIdNum = mFlowId.getId();
        String flowSourceId = "rtengine-flow-" + flowIdNum + "-" + streamSymbol.getName();
        newElem = new LocalFlumeSinkElement(newContext, flowSourceId,
            mFlumeConfig, flumeSource);
        if (!streamSymbol.isLocal()) {
          LOG.info("Created local Flume logical node: " + flowSourceId);
          LOG.info("You may need to connect upstream Flume elements to this source.");
        }
        break;
      default:
        // TODO(aaron): Turn this into a checked exception when we have an API for it.
        throw new RuntimeException("Unhandled stream source type: "
            + streamSymbol.getSourceType());
      }
    } else if (node instanceof StrMatchFilterNode) {
      StrMatchFilterNode matchNode = (StrMatchFilterNode) node;
      String matchStr = matchNode.getMatchString();
      newElem = new StrMatchFilterElement(newContext, matchStr);
    } else {
      LOG.error("Cannot create FlowElement for PlanNode of type: " + node.getClass().getName());
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
