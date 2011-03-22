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

package com.odiago.flumebase.plan;

import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.util.DAG;
import com.odiago.flumebase.util.DAGOperatorException;

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
 *   <li>If a node has multiple input schemas, each of the precedessors should
 *   match one of the node's inputs.</li>
 *   <li>The output schema of this node, if unset, should be set to match the
 *   input schema. Fail if a node has multiple input schemas.</li>
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

    // A node is either defined as having one well-specified input schema, or a list
    // of options. If the list is set, each predecessor output must match one of the
    // options.
    List<Schema> myInputSchemas = (List<Schema>)
        node.getAttr(PlanNode.MULTI_INPUT_SCHEMA_ATTR);
    Schema inputSchema = null;
    for (PlanNode parent : parents) {
      Schema parentOutputSchema = (Schema) parent.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      if (null == parentOutputSchema) {
        // This should not happen if we use this operator correctly with BFS.
        throw new DAGOperatorException("Node " + parent + " does not have output schema set");
      }

      if (myInputSchemas != null) {
        // This node accepts multiple input schemas. Check that the parent's output
        // schema matches one of them.
        boolean match = false;
        for (Schema candidate : myInputSchemas) {
          if (candidate.equals(parentOutputSchema)) {
            match = true;
            break;
          }
        }

        if (!match) {
          throw new DAGOperatorException("Schema resolution execption; node [" + node
              + "] has a parent output schema that does not match any candidate input schema.");
        }
      } else if (null == inputSchema) {
        // This node will have a single input schema. Cache the first parent's output schema.
        inputSchema = parentOutputSchema;
      } else {
        // Now check that each other parent has the same schema as the first parent.
        if (!parentOutputSchema.equals(inputSchema)) {
          throw new DAGOperatorException("Schema resolution exception; node [" + node
              + "] has parents with mismatched schemas:\nSchema 1:\n"
              + inputSchema + "\nSchema 2:\n" + parentOutputSchema);
        }
      }
    }

    if (myInputSchemas == null) {
      // For nodes with exactly one input schema, check that the defined input
      // schema (if any) matches the output of the predecessors. If unset,
      // set it to the predecessor output.
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

      // And if there's no output schema defined, set it equal to our input schema.
      Schema myOutputSchema = (Schema) node.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR);
      if (null == myOutputSchema) {
        node.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, myInputSchema);
      }
    } else {
      // For nodes that accept multiple input schemas, just check that the
      // output schema is set, since we can't infer the output schema from the set
      // of input schemas.
      if (null == node.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR)) {
        throw new DAGOperatorException("Node [" + node
            + "] has multiple input schemas but the output schema is unset.");
      }

      // Sanity check: If this also has a singleton input schema set, complain.
      if (null != node.getAttr(PlanNode.INPUT_SCHEMA_ATTR)) {
        throw new DAGOperatorException("Node [" + node
            + "] has multiple input schemas and singleton input schema set.");
      }
    }
  }

  
}
