// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.exec.local;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.EvaluationElement;
import com.odiago.rtengine.exec.FlowElement;
import com.odiago.rtengine.exec.FlowElementContext;
import com.odiago.rtengine.exec.FlowId;
import com.odiago.rtengine.exec.InMemStreamSymbol;
import com.odiago.rtengine.exec.ProjectionElement;
import com.odiago.rtengine.exec.FilterElement;
import com.odiago.rtengine.exec.StreamSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.flume.EmbeddedFlumeConfig;

import com.odiago.rtengine.parser.EntityTarget;
import com.odiago.rtengine.parser.Expr;

import com.odiago.rtengine.plan.ConsoleOutputNode;
import com.odiago.rtengine.plan.CreateStreamNode;
import com.odiago.rtengine.plan.DescribeNode;
import com.odiago.rtengine.plan.DropNode;
import com.odiago.rtengine.plan.EvaluateExprsNode;
import com.odiago.rtengine.plan.MemoryOutputNode;
import com.odiago.rtengine.plan.NamedSourceNode;
import com.odiago.rtengine.plan.PlanNode;
import com.odiago.rtengine.plan.ProjectionNode;
import com.odiago.rtengine.plan.FilterNode;

import com.odiago.rtengine.util.DAG;
import com.odiago.rtengine.util.DAGOperatorException;

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

  public LocalFlowBuilder(FlowId flowId, SymbolTable rootSymTable,
      EmbeddedFlumeConfig flumeConfig, Map<String, MemoryOutputElement> memOutputMap) {
    mFlowId = flowId;
    mMemOutputMap = memOutputMap;
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
      return new SinkFlowElemContext(mFlowId);
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

  public void process(PlanNode node) throws DAGOperatorException {
    FlowElement newElem = null; // The newly-constructed FlowElement.
    FlowElementContext newContext = makeContextForNode(node);

    if (null == node) {
      LOG.warn("Null node in plan graph");
      return;
    } else if (node instanceof ConsoleOutputNode) {
      ConsoleOutputNode consoleNode = (ConsoleOutputNode) node;
      newElem = new ConsoleOutputElement(newContext,
          (Schema) consoleNode.getAttr(PlanNode.INPUT_SCHEMA_ATTR),
          consoleNode.getFields());
    } else if (node instanceof MemoryOutputNode) {
      MemoryOutputNode memoryNode = (MemoryOutputNode) node;
      newElem = new MemoryOutputElement(newContext,
          (Schema) memoryNode.getAttr(PlanNode.INPUT_SCHEMA_ATTR),
          memoryNode.getFields());
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
      if (mRootSymbolTable.resolve(streamName) != null) {
        // TODO: Allow CREATE OR REPLACE STREAM to override this.
        throw new DAGOperatorException("Object already exists at top level: " + streamName);
      } else {
        mRootSymbolTable.addSymbol(streamSym);
        System.out.println("CREATE STREAM");
      }
    } else if (node instanceof DescribeNode) {
      // Look up the referenced object in the symbol table and describe it immediately.
      DescribeNode describe = (DescribeNode) node;
      Symbol sym = mRootSymbolTable.resolve(describe.getIdentifier());
      System.out.println(sym);
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
      System.out.println("DROP " + targetType.toString().toUpperCase());
    } else if (node instanceof NamedSourceNode) {
      NamedSourceNode namedInput = (NamedSourceNode) node;
      String streamName = namedInput.getStreamName();
      Symbol symbol = mRootSymbolTable.resolve(streamName);
      if (null == symbol) {
        throw new DAGOperatorException("No symbol for stream: " + streamName);
      }

      if (!(symbol instanceof StreamSymbol)) {
        throw new DAGOperatorException("Identifier " + streamName + " has type: "
            + symbol.getType() + ", not STREAM.");
      }

      StreamSymbol streamSymbol = (StreamSymbol) symbol;
      if (!streamSymbol.isLocal()) {
        throw new DAGOperatorException("Do not know how to handle a non-local source yet.");
      }

      switch (streamSymbol.getSourceType()) {
      case File:
        String fileName = streamSymbol.getSource();
        newElem = new LocalFileSourceElement(newContext, fileName,
            namedInput.getFields(), streamSymbol);
        break;
      case Sink:
        String flumeSource = streamSymbol.getSource();
        long flowIdNum = mFlowId.getId();
        String flowSourceId = "rtengine-flow-" + flowIdNum + "-" + streamSymbol.getName();
        newElem = new LocalFlumeSinkElement(newContext, flowSourceId,
            mFlumeConfig, flumeSource, (Schema) namedInput.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR),
            namedInput.getFields());
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
    } else if (node instanceof EvaluateExprsNode) {
      EvaluateExprsNode evalNode = (EvaluateExprsNode) node;
      Schema outSchema = (Schema) evalNode.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      newElem = new EvaluationElement(newContext, evalNode.getExprs(),
          evalNode.getPropagateFields(), outSchema);
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
