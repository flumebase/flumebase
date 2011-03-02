// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.util;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static com.odiago.rtengine.testutil.RtsqlAsserts.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test that the DAG operators work correctly. */
public class TestDAG {
  private static final Logger LOG = LoggerFactory.getLogger(TestDAG.class.getName());

  private static class Node extends DAGNode<Node> {
    public Node(int id) {
      super(id);
    }
  }

  /**
   * Creates a diamond of dag nodes:
   * <p><pre><tt>
   *        1
   *       / \
   *      2   3
   *       \ /
   *        4
   * </tt></pre></p>
   */
  private DAG<Node> createDiamond() {
    DAG<Node> dag = new DAG<Node>();
    Node root = new Node(1);
    dag.addRoot(root);
    Node leftChild = new Node(2);
    root.addChild(leftChild);
    Node rightChild = new Node(3);
    root.addChild(rightChild);
    Node bottom = new Node(4);
    leftChild.addChild(bottom);
    rightChild.addChild(bottom);
    return dag;
  }

  private static class WalkOrderOperator extends DAG.Operator<Node> {
    /** list of node ids, in the order we visited them. */
    private List<Integer> mIds;

    public WalkOrderOperator() {
      mIds = new ArrayList<Integer>();
    }

    @Override
    public void process(Node node) {
      LOG.info("Processing node: " + node.getId());
      mIds.add(node.getId());
    }

    public int[] getIds() {
      int[] out = new int[mIds.size()];

      for (int i = 0; i < mIds.size(); i++) {
        out[i] = mIds.get(i);
      }
      return out;
    }
  }

  /** Test that the diamond DAG has the internal structure we expect. */
  @Test
  public void testDiamondStructure() throws Exception {
    DAG<Node> diamond = createDiamond();

    // Assert that only node '1' is a root.
    List<Node> roots = diamond.getRootSet();
    assertEquals(1, roots.size());
    assertEquals(1, roots.get(0).getId());

    // Assert that only node '4' is at the bottom of the DAG.
    List<Node> bottoms = diamond.getLastLayer();
    assertEquals(1, bottoms.size());
    assertEquals(4, bottoms.get(0).getId());

    Node bottom = bottoms.get(0);

    Node root = roots.get(0);
    Node left = root.getChildren().get(0);
    Node right = root.getChildren().get(1);
    assertEquals(2, root.getChildren().size());
    assertEquals(1, left.getParents().size());
    assertEquals(1, right.getParents().size());
    assertEquals(root, left.getParents().get(0));
    assertEquals(root, right.getParents().get(0));

    assertEquals(0, bottom.getChildren().size());
    assertEquals(2, bottom.getParents().size());

    assertTrue(bottom.getParents().contains(left));
    assertTrue(bottom.getParents().contains(right));
  }

  /** Test that traversals over the diamond DAG act like we expect. */
  @Test
  public void testDiamondOperators() throws Exception {
    LOG.info("BFS");
    DAG<Node> diamond = createDiamond();
    WalkOrderOperator op = new WalkOrderOperator();
    diamond.bfs(op);
    int[] bfsOrder = { 1, 2, 3, 4 };
    assertArrayEquals(bfsOrder, op.getIds());

    LOG.info("DFS");
    op = new WalkOrderOperator();
    diamond.dfs(op);
    int[] dfsOrder = { 1, 3, 4, 2 };
    assertArrayEquals(dfsOrder, op.getIds());

    LOG.info("ReverseBFS");
    op = new WalkOrderOperator();
    diamond.reverseBfs(op);
    int[] reverseBfsOrder = { 4, 2, 3, 1 };
    assertArrayEquals(reverseBfsOrder, op.getIds());

  }

  /**
   * BFS does not necessarily hit nodes in rank order, but rankTraversal()
   * should.
   * Make sure we reach these nodes in order 1, 2, 3, not 1, 3, 2:
   *
   * <p><pre><tt>
   *        1
   *       / \
   *      (   2
   *       \ /
   *        3
   * </tt></pre></p>
   */
  @Test
  public void testRankLookahead() throws Exception {
    // Create the graph..
    DAG<Node> graph = new DAG<Node>();
    Node root = new Node(1);
    graph.addRoot(root);
    Node bottom = new Node(3);
    root.addChild(bottom);
    Node middle = new Node(2);
    root.addChild(middle);
    middle.addChild(bottom);

    WalkOrderOperator op = new WalkOrderOperator();
    graph.rankTraversal(op);
    int[] rankOrder = { 1, 2, 3 };
    assertArrayEquals(rankOrder, op.getIds());
  }

  /**
   * Test rankTraversal() on a deeper graph.
   *
   * <p><pre><tt>
   *        1
   *       / \
   *      (   2
   *       \ /
   *        3
   *       / \
   *      4   )
   *       \ /
   *        5
   * </tt></pre></p>
   */
  @Test
  public void testRankLookahead2() throws Exception {
    // Create the graph..
    DAG<Node> graph = new DAG<Node>();
    Node root = new Node(1);
    graph.addRoot(root);
    Node pivot = new Node(3);
    root.addChild(pivot);
    Node middle = new Node(2);
    root.addChild(middle);
    middle.addChild(pivot);

    Node left = new Node(4);
    pivot.addChild(left);
    Node last = new Node(5);
    pivot.addChild(last);
    left.addChild(last);

    WalkOrderOperator op = new WalkOrderOperator();
    graph.rankTraversal(op);
    int[] rankOrder = { 1, 2, 3, 4, 5 };
    assertArrayEquals(rankOrder, op.getIds());
  }
}
