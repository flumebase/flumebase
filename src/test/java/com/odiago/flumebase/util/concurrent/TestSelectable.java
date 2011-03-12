// (c) Copyright 2011 Odiago, Inc.

package com.odiago.flumebase.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.testng.annotations.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.testng.AssertJUnit.*;

/**
 * Run tests with multiple producers pushing values into Selectable
 * objects; have a consumer using a Select to read from all of them.
 */
public class TestSelectable {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestSelectable.class.getName());

  /**
   * Sequentially produce integers into a queue.
   */
  private class ProducerThread extends Thread {
    private SelectableQueue<Long> mQueue;

    /** How many values to produce before shutting down. */
    private int mNumVals;

    public ProducerThread(SelectableQueue<Long> queue, int numVals) {
      mQueue = queue;
      mNumVals = numVals;
    }

    public void run() {
      // There is a 1% chance that we sleep after each put, to randomize the
      // thread collisions a bit.

      Random r = new Random(System.currentTimeMillis() + hashCode());
      for (int i = 1; i <= mNumVals; i++) {
        try {
          mQueue.put(Long.valueOf(i));
        } catch (InterruptedException ie) {
          LOG.info("Interrupted in producer!");
        }

        if (r.nextInt(100) < 2) {
          try {
            Thread.sleep(1);
          } catch (InterruptedException ie) {
            // Ignore this.
          }
        }
      }
    }
  }

  /**
   * Retrieve integers from a queue until we receive a '0'.
   */
  private class ConsumerThread extends Thread {
    /** Select interface to poll. */
    private Select<Long> mSelect;

    /** Sum of all values received thus far. */
    private long mTotal;
    
    /** Number of values received so far. */
    private int mValsReceived;

    /** Number of values we expect to receive. */
    private int mExpectedNumVals;

    public ConsumerThread(Select<Long> select, int numQueues, int valsPerQueue) {
      mSelect = select;
      mTotal = 0;
      mValsReceived = 0;
      mExpectedNumVals = numQueues * valsPerQueue;
    }

    public void run() {
      while (mValsReceived < mExpectedNumVals) {
        Long val = null;
        try {
          val = mSelect.read();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted!");
          continue;
        }

        assertNotNull(val);

        mTotal += val.longValue();
        mValsReceived++;
      }
    }

    public long getTotal() {
      return mTotal;
    }
  }

  /**
   * Create a set of producer threads with individual queues, and a consumer
   * thread that selects on all of them.
   * Each producer will insert the values [1, numVals] into its queue.
   * The consumer tallies up the values received. The total value received
   * should be numProducers * (numVals + 1) * numVals / 2,
   * since the sum of all values from [1, x] is (x+1)x/2.
   */
  public void runTest(List<SelectableQueue<Long>> queues, int numValsPerProducer) {
    Select<Long> select = new Select<Long>();
    List<ProducerThread> producers = new ArrayList<ProducerThread>();
    for (SelectableQueue<Long> queue : queues) {
      producers.add(new ProducerThread(queue, numValsPerProducer));
      select.add(queue);
    }

    ConsumerThread consumer = new ConsumerThread(select, queues.size(), numValsPerProducer);

    for (ProducerThread prod : producers) {
      prod.start();
    }

    consumer.start();

    // Wait for the consumer to wrap up, and check the total.

    long finalTotal = 0;
    try {
      consumer.join();
      finalTotal += consumer.getTotal();
    } catch (InterruptedException ie) {
      LOG.info("Interrupted during join()");
    }

    // Assert that the queue is fully drained.
    for (SelectableQueue<Long> queue : queues) {
      assertEquals("queue not drained", 0, queue.size());
    }

    long expectedTotal = (long) queues.size() * (long) numValsPerProducer
        * ((long) numValsPerProducer + 1L) / 2L;
    assertEquals(expectedTotal, finalTotal);
  }

  @Test
  public void simpleTest() {
    // single producer no size bound.
    List<SelectableQueue<Long>> queues = new ArrayList<SelectableQueue<Long>>();
    queues.add(new SyncSelectableQueue<Long>());
    runTest(queues, 100);
  }

  @Test
  public void testTwoQueues() {
    // two bounded queues.
    List<SelectableQueue<Long>> queues = new ArrayList<SelectableQueue<Long>>();
    queues.add(new ArrayBoundedSelectableQueue<Long>(50));
    queues.add(new ArrayBoundedSelectableQueue<Long>(50));
    runTest(queues, 500);
  }

  @Test
  public void testFourQueues() {
    // two bounded queues, two infinite queues
    List<SelectableQueue<Long>> queues = new ArrayList<SelectableQueue<Long>>();
    queues.add(new ArrayBoundedSelectableQueue<Long>(50));
    queues.add(new ArrayBoundedSelectableQueue<Long>(50));
    queues.add(new SyncSelectableQueue<Long>());
    queues.add(new SyncSelectableQueue<Long>());
    runTest(queues, 500);
  }
}
