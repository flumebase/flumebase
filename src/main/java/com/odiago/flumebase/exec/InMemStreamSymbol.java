// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.lang.StreamType;

import com.odiago.flumebase.parser.FormatSpec;
import com.odiago.flumebase.parser.StreamSourceType;
import com.odiago.flumebase.parser.TypedField;

/**
 * A symbol representing a stream whose input contents are entirely
 * held in memory ahead of time. This is used mostly for testing.
 */
public class InMemStreamSymbol extends StreamSymbol {
  private static final Logger LOG = LoggerFactory.getLogger(
      InMemStreamSymbol.class.getName());

  /**
   * Specifies how quickly the source element should emit the events of this
   * stream; allows simulation of different "real-world" rates at which data
   * will enter the system.
   */
  public static enum LatencyPolicy {
    None,      // Just emit values as quickly as possible.
    Timestamp, // Mimic the actual timestamps of events; sleep between events
               // proportional to the actual time interval between these inputs.
    MildLag,   // Like Timestamp, but add a variable (but small) amount of jitter.
    HeavyLag,  // Add a very large amount of lag between events; push events past
               // the slack time interval.
    Avalanche, // At some time mid-stream, sleep for a long time, to simulate an
               // upstream failure, then blast events quickly as if the upstream
               // element is back online and retransmitting lost events.

  }

  /** The set of events to replay as the contents of the stream. */
  private List<Event> mInputEvents;

  /** Latency policy applied to this stream's delivery. */
  private LatencyPolicy mLatencyPolicy;

  /**
   * Iterator that wraps the underlying event iterator with one
   * that adds latency to the event delivery based on the configured
   * latency policy.
   */
  private class DeliveryIterator implements Iterator<Event> {
    private Iterator<Event> mIter;

    private long mPrevTimestamp; // Timestamp of previously-returned element.
    private long mPrevReturnTime; // Local time at which we returned the previous element.

    private boolean mAvalanched; // true if we triggered an avalanche already.

    private Random mRandom;

    public DeliveryIterator(Iterator<Event> iter) {
      mIter = iter;
      mRandom = new Random(System.currentTimeMillis());
      mAvalanched = false;
    }

    @Override
    public boolean hasNext() {
      return mIter.hasNext();
    }

    /**
     * Decide at a random point during the data stream to cause the upstream
     * source to intermittently fail, and then trigger a resend of lots of data.
     * @return true if we triggered an avalanche, false otherwise.
     */
    private boolean causeAvalanche() {
      if (mRandom.nextInt(1000) < 200) {
        // 20% chance of causing an avalanche on any given event.
        mAvalanched = true;
      }

      return mAvalanched;
    }

    /** @return the amount of time we need to sleep in order to return the next
     * event at a delay equal to the wall-clock interval between the prev event's
     * timestamp and the next event's timestamp.
     */
    private long getNapTime(Event nextEvent) {
      if (mPrevReturnTime != 0) {
        long totalInterval = nextEvent.getTimestamp() - mPrevTimestamp;
        long curTime = System.currentTimeMillis();
        long consumedInterval = curTime - mPrevReturnTime;
        long napTime = totalInterval - consumedInterval;
        LOG.info("NAP TIME: " + napTime);
        return napTime;
      } else {
        return 0; // No previous event; return immediately.
      }
    }

    @Override
    public Event next() {
      Event nextEvent = mIter.next();

      try {
        switch (InMemStreamSymbol.this.mLatencyPolicy) {
        case None:
          break; // No delay required.
        case Timestamp:
          // Sleep based on the timestamps of the nextEvent and the prev event.
          Thread.sleep(getNapTime(nextEvent));
          break;
        case MildLag:
          // Add up to 40 ms of lag.
          Thread.sleep(getNapTime(nextEvent) + mRandom.nextInt(40));
          break;
        case HeavyLag:
          // Add up to 500 ms of lag.
          Thread.sleep(getNapTime(nextEvent) + mRandom.nextInt(500));
          break;
        case Avalanche:
          if (mAvalanched) {
            // Avalanche already occurred; dump data as fast as possible.
            break; 
          } else if (causeAvalanche()) {
            // We just triggered an avalanche. Sleep two full seconds.
            Thread.sleep(2000);
          } else {
            // Avalanche has not yet happened. Return things in 'real time.'
            Thread.sleep(getNapTime(nextEvent));
          }
          break;
        default:
          throw new RuntimeException("Unexpected latency policy: " + mLatencyPolicy);
        }
      } catch (InterruptedException ie) {
        // ignore any interrupt in here; just return the next event
        // quickly to the upstream caller.
      }

      mPrevTimestamp = nextEvent.getTimestamp();
      mPrevReturnTime = System.currentTimeMillis();
      return nextEvent;
    }

    @Override
    public void remove() {
      mIter.remove();
    }
  }

  public InMemStreamSymbol(String name, StreamType type, List<Event> inputEvents,
      List<TypedField> fields, FormatSpec format, LatencyPolicy latency) {
    super(name, StreamSourceType.Memory, type, null, true, fields, format);
    mInputEvents = inputEvents;
    mLatencyPolicy = latency;

    assert null != mInputEvents;
    assert null != mLatencyPolicy;
  }

  public Iterator<Event> getEvents() {
    return this.new DeliveryIterator(mInputEvents.iterator());
  }

  public LatencyPolicy getLatencyPolicy() {
    return mLatencyPolicy;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(super.toString());
    sb.append("  records:\n");
    for (Event record : mInputEvents) {
      sb.append("    \"");
      sb.append(new String(record.getBody()));
      sb.append("\"\n");
    }

    return sb.toString();
  }
}
