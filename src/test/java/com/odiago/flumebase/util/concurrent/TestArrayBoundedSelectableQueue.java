// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util.concurrent;

import org.testng.annotations.Test;

public class TestArrayBoundedSelectableQueue extends QueueTestCase {
  @Test
  public void TestSimple() {
    // 1 producer, 1 consumer, bigger buffer than we need.
    runTest(new ArrayBoundedSelectableQueue<Long>(500), 1, 1, 100);

    // 1 producer, 1 consumer, smaller buffer than we need.
    runTest(new ArrayBoundedSelectableQueue<Long>(20), 1, 1, 100);
  }

  @Test
  public void testFanOut() {
    // 1 producer, 2 consumers
    runTest(new ArrayBoundedSelectableQueue<Long>(100), 1, 2, 10000);

    // 1 producer, 4 consumers
    runTest(new ArrayBoundedSelectableQueue<Long>(1500), 1, 4, 25000);
  }

  @Test
  public void testFanIn() {
    // 2 producers, 1 consumer
    runTest(new ArrayBoundedSelectableQueue<Long>(100), 2, 1, 10000);

    // 1 producer, 4 consumers
    runTest(new ArrayBoundedSelectableQueue<Long>(1500), 4, 1, 25000);
  }

  @Test
  public void testMultiMulti() {
    // 2 producers, 2 consumers
    runTest(new ArrayBoundedSelectableQueue<Long>(100), 2, 2, 10000);

    // 3 producers, 3 consumers
    runTest(new ArrayBoundedSelectableQueue<Long>(1500), 3, 3, 25000);
  }
}
