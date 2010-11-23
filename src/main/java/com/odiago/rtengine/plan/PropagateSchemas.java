// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.util.DAG;
import com.odiago.rtengine.util.DAGOperatorException;

/**
 * With certain components of the DAG marked with input and output
 * schemas, flow this information forward to all nodes in the DAG.
 *
 * <p>At each node:
 * <ul>
 *   <li>The output schemas from each predecessor node should be identical.
 *   Fail if they are not.</li>
 *   <li>The input schema of this node should match the output schemas from
 *   the predecessor nodes. Fail if this is not. If this is unset, copy from
 *   a predecssor node.</li>
 *   <li>The output schema of this node, if unset, should be set to match the
 *   input schema.</li>
 * </ul>
 * </p>
 *
 * This is a DAG operator to be used with bfs after logical plan formation
 * from the AST, but before physical plan resolution.
 */
public class PropagateSchemas extends DAG.Operator<PlanNode> {
  private static final Logger LOG = LoggerFactory.getLogger(
      PropagateSchemas.class.getName());

  @Override
  public void process(PlanNode node) throws DAGOperatorException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Operating on node: [" + node + "]");
    }

    List<PlanNode> parents = node.getParents();
    Schema inputSchema = null;
    for (PlanNode parent : parents) {
      Schema parentOutputSchema = (Schema) parent.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      if (null == parentOutputSchema) {
        // This should not happen if we use this operator correctly with BFS.
        throw new DAGOperatorException("Node " + parent + " does not have output schema set");
      }
      if (null == inputSchema) {
        // Catch the first parent's output schema.
        inputSchema = parentOutputSchema;
      } else {
        // Now check that each other parent has the same schema.
        if (!parentOutputSchema.equals(inputSchema)) {
          throw new DAGOperatorException("Schema resolution exception; node [" + node
              + "] has parents with mismatched schemas:\nSchema 1:\n"
              + inputSchema + "\nSchema 2:\n" + parentOutputSchema);
        }
      }
    }

    Schema myInputSchema = (Schema) node.getAttr(PlanNode.INPUT_SCHEMA_ATTR);
    if (null == myInputSchema) {
      node.setAttr(PlanNode.INPUT_SCHEMA_ATTR, inputSchema);
      myInputSchema = inputSchema;
    }

    if (null != myInputSchema && null != inputSchema) {
      // Check that these are equal.
      if (!inputSchema.equals(myInputSchema)) {
        throw new DAGOperatorException("Node [" + node + "] has set input schema:\n"
            + myInputSchema + "\nbut parents have output schema:\n" + inputSchema);
      }
    }

    Schema myOutputSchema = (Schema) node.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
    if (null == myOutputSchema) {
      node.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, myInputSchema);
    }
  }

  
}
