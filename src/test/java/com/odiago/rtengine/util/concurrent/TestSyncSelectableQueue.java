// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

import org.junit.Test;

public class TestSyncSelectableQueue extends QueueTestCase {
  @Test
  public void TestSimple() {
    // 1 producer, 1 consumer
    runTest(new SyncSelectableQueue<Long>(), 1, 1, 100);
  }

  @Test
  public void testFanOut() {
    // 1 producer, 2 consumers
    runTest(new SyncSelectableQueue<Long>(), 1, 2, 10000);

    // 1 producer, 4 consumers
    runTest(new SyncSelectableQueue<Long>(), 1, 4, 25000);
  }

  @Test
  public void testFanIn() {
    // 2 producers, 1 consumer
    runTest(new SyncSelectableQueue<Long>(), 2, 1, 10000);

    // 1 producer, 4 consumers
    runTest(new SyncSelectableQueue<Long>(), 4, 1, 25000);
  }

  @Test
  public void testMultiMulti() {
    // 2 producers, 2 consumers
    runTest(new SyncSelectableQueue<Long>(), 2, 2, 10000);

    // 3 producers, 3 consumers
    runTest(new SyncSelectableQueue<Long>(), 3, 3, 25000);
  }
}
