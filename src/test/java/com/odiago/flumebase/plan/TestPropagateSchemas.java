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

import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;

import org.testng.annotations.Test;

import com.odiago.flumebase.util.DAGOperatorException;

import static org.testng.AssertJUnit.*;

/** Test that the PropagateSchemas BFS pass works correctly. */
public class TestPropagateSchemas {

  @Test
  public void testOneNode() throws Exception {
    // Test that a single node propagates its input schema to its output schema.
    Schema input = Schema.create(Schema.Type.INT);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode node = new PlanNode();
    node.setAttr(PlanNode.INPUT_SCHEMA_ATTR, input);
    spec.addRoot(node);
    spec.bfs(new PropagateSchemas());
    assertEquals(input, node.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
  }

  @Test
  public void testCarryOneForward() throws Exception {
    // Given A->B, test that A's input schema is set as B's output schema.
    Schema input = Schema.create(Schema.Type.INT);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode nodeA = new PlanNode();
    nodeA.setAttr(PlanNode.INPUT_SCHEMA_ATTR, input);
    PlanNode nodeB = new PlanNode();
    nodeA.addChild(nodeB);
    spec.addRoot(nodeA);
    spec.bfs(new PropagateSchemas());
    assertEquals(input, nodeB.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
  }

  @Test(expectedExceptions = DAGOperatorException.class)
  public void testMismatchedPredecessors() throws Exception {
    // Given mismatched predecessor output schemas, test that we throw an
    // exception in the bfs.
    Schema leftOutput = Schema.create(Schema.Type.INT);
    Schema rightOutput = Schema.create(Schema.Type.STRING);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode leftNode = new PlanNode();
    PlanNode rightNode = new PlanNode();
    leftNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, leftOutput);
    rightNode.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, rightOutput);
    spec.addRoot(leftNode);
    spec.addRoot(rightNode);

    PlanNode child = new PlanNode();
    spec.attachToLastLayer(child);
    spec.bfs(new PropagateSchemas()); // Throws exception
  }

  @Test(expectedExceptions = DAGOperatorException.class)
  public void testMismatchedOutputInput() throws Exception {
    // Given a predecessor output schema that doesn't match our input schema,
    // test that we signal error.
    Schema output = Schema.create(Schema.Type.INT);
    Schema input = Schema.create(Schema.Type.STRING);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode parent = new PlanNode();
    parent.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, output);
    PlanNode child = new PlanNode();
    child.setAttr(PlanNode.INPUT_SCHEMA_ATTR, input);
    parent.addChild(child);
    spec.addRoot(parent);
    spec.bfs(new PropagateSchemas()); // Throws exception.
  }

  @Test
  public void testInterruptedPropagation() throws Exception {
    // Given A->B->C, with A's input and B's output schemas set (and
    // different), test that everything propagates correctly.
    Schema inputA = Schema.create(Schema.Type.INT);
    Schema outputB = Schema.create(Schema.Type.STRING);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode nodeA = new PlanNode();
    nodeA.setAttr(PlanNode.INPUT_SCHEMA_ATTR, inputA);
    PlanNode nodeB = new PlanNode();
    nodeB.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputB);
    nodeA.addChild(nodeB);
    PlanNode nodeC = new PlanNode();
    nodeB.addChild(nodeC);
    spec.addRoot(nodeA);
    spec.bfs(new PropagateSchemas());

    assertEquals(inputA, (Schema) nodeA.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(inputA, (Schema) nodeA.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(inputA, (Schema) nodeB.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeB.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
  }

  @Test
  public void testUnchangedNode() throws Exception {

    // If both input and output schemas for a single node are set, do not
    // change anything.
    Schema inputB = Schema.create(Schema.Type.INT);
    Schema outputB = Schema.create(Schema.Type.STRING);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode nodeA = new PlanNode();
    nodeA.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, inputB);
    PlanNode nodeB = new PlanNode();
    nodeB.setAttr(PlanNode.INPUT_SCHEMA_ATTR, inputB);
    nodeB.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputB);
    nodeA.addChild(nodeB);
    PlanNode nodeC = new PlanNode();
    nodeB.addChild(nodeC);
    spec.addRoot(nodeA);
    spec.bfs(new PropagateSchemas());

    assertNull(nodeA.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(inputB, (Schema) nodeA.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(inputB, (Schema) nodeB.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeB.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
  }

  @Test
  public void testUnchangedNodeWithPrior() throws Exception {

    // If both input and output schemas for a single node are set, do not
    // change anything, even if the predecessor node has a (matching) input set.
    Schema inputB = Schema.create(Schema.Type.INT);
    Schema outputB = Schema.create(Schema.Type.STRING);
    FlowSpecification spec = new FlowSpecification(new Configuration());
    PlanNode nodeA = new PlanNode();
    nodeA.setAttr(PlanNode.INPUT_SCHEMA_ATTR, inputB);
    PlanNode nodeB = new PlanNode();
    nodeB.setAttr(PlanNode.INPUT_SCHEMA_ATTR, inputB);
    nodeB.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outputB);
    nodeA.addChild(nodeB);
    PlanNode nodeC = new PlanNode();
    nodeB.addChild(nodeC);
    spec.addRoot(nodeA);
    spec.bfs(new PropagateSchemas());

    assertEquals(inputB, (Schema) nodeA.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(inputB, (Schema) nodeA.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(inputB, (Schema) nodeB.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeB.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.INPUT_SCHEMA_ATTR));
    assertEquals(outputB, (Schema) nodeC.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));
  }

}
