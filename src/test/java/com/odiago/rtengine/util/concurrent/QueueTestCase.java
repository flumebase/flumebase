// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class QueueTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(
      QueueTestCase.class.getName());

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
    private SelectableQueue<Long> mQueue;

    /** Sum of all values received thus far. */
    private long mTotal;

    public ConsumerThread(SelectableQueue<Long> queue) {
      mQueue = queue;
    }

    public void run() {
      while (true) {
        Long val = null;
        try {
          val = mQueue.take();
        } catch (InterruptedException ie) {
          LOG.info("Interrupted in consumer");
        }

        if (null == val) {
          continue;
        } else if (val.longValue() == 0) {
          // We're done.
          return;
        } else {
          mTotal += val.longValue();
        }
      }
    }

    public long getTotal() {
      return mTotal;
    }
  }

  /**
   * Create a set of producer and consumer threads and turn them loose on a queue.
   * Each producer will insert the values [1, numVals] into the queue.
   * Each consumer tallies up the values received. The total value received
   * over all consumers should be numProducers * (numVals + 1) * numVals / 2,
   * since the sum of all values from [1, x] is (x+1)x/2.
   */
  public void runTest(SelectableQueue<Long> queue, int numProducers, int numConsumers,
      int numValsPerProducer) {
    List<ProducerThread> producers = new ArrayList<ProducerThread>();
    for (int i = 0; i < numProducers; i++) {
      producers.add(new ProducerThread(queue, numValsPerProducer));
    }

    List<ConsumerThread> consumers = new ArrayList<ConsumerThread>();
    for (int i = 0; i < numConsumers; i++) {
      consumers.add(new ConsumerThread(queue));
    }

    for (ProducerThread prod : producers) {
      prod.start();
    }

    for (ConsumerThread con : consumers) {
      con.start();
    }

    // Wait for the producers to finish.
    for (ProducerThread prod : producers) {
      try {
        prod.join();
      } catch (InterruptedException ie) {
        LOG.info("Interrupted during producer join");
      }
    }

    // Now the queue is "complete". Put as many 0's into the queue as there
    // are consumers, to tell them all to quit.
    for (int i = 0; i < numConsumers; i++) {
      try {
        queue.put(Long.valueOf(0));
      } catch (InterruptedException ie) {
        LOG.info("Interrupted putting zero to queue!");
        i--; // try again.
      }
    }

    // Wait for the consumers to wrap up, and check their tallies.

    long finalTotal = 0;
    for (ConsumerThread con : consumers) {
      try {
        con.join();
        finalTotal += con.getTotal();
      } catch (InterruptedException ie) {
        LOG.info("Interrupted during join()");
      }
    }

    // Assert that the queue is fully drained.
    assertEquals("queue not drained", 0, queue.size());

    long expectedTotal = (long) numProducers * (long) numValsPerProducer
        * ((long) numValsPerProducer + 1L) / 2L;
    assertEquals(expectedTotal, finalTotal);
  }

  // mvn surefire test runner complains if this does not contain at least one test.
  @Test
  public void ignoredTestCase() { }
}
