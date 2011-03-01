// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.PriorityBlockingQueue;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Pair;

import com.odiago.rtengine.exec.local.TimerFlowElemContext;

import com.odiago.rtengine.lang.TimeSpan;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.TypedField;
import com.odiago.rtengine.parser.WindowSpec;

import com.odiago.rtengine.plan.AggregateNode;
import com.odiago.rtengine.plan.PlanNode;

import com.odiago.rtengine.util.IterableIterator;
import com.odiago.rtengine.util.PairLeftRightComparator;

import com.odiago.rtengine.util.concurrent.SelectableQueue;

@SuppressWarnings("rawtypes")
/**
 * Perform aggregation functions over time series data divided into
 * a fixed number of buckets over the aggregation time interval.
 */
public class BucketedAggregationElement extends AvroOutputElementImpl {
  private static final Logger LOG = LoggerFactory.getLogger(
      BucketedAggregationElement.class.getName());

  /** Configuration key for the number of buckets that subdivide the aggregation time interval. */
  private static final String NUM_BUCKETS_KEY = "rtsql.aggregation.buckets";
  private static final int DEFAULT_NUM_BUCKETS = 100;

  /**
   * Configuration key specifying whether continuous output should be used.
   * If true, output should be generated for every bucket interval, even if no new data
   * is available in that bucket; if false, only generate output when the input
   * condition changes.
   */
  private static final String CONTINUOUS_OUTPUT_KEY = "rtsql.aggregation.continuous.output";
  private static final boolean DEFAULT_CONTINUOUS_OUTPUT = false;

  /**
   * Configuration key specifying how far in the past we emit as output when
   * an insertion forces a close/emit of prior time windows.
   * If there's a massive stall, don't worry about data that is more than
   * this many milliseconds old.
   */
  private static final String MAX_PRIOR_EMIT_INTERVAL_KEY =
      "rtsql.aggregation.max.prior.interval";
  private static final long DEFAULT_MAX_PRIOR_EMIT_INTERVAL = 5000;

  /** The number of buckets that subdivide the aggregation time interval. */
  private final int mNumBuckets;

  /** Indicates whether continuous output is enabled. */
  private final boolean mContinuousOutput;

  /** How far into the past we will look for windows to close when catching up to the present. */
  private final long mMaxPriorEmitInterval;

  private final List<TypedField> mGroupByFields;

  /** The window specification over which we're aggregating. */
  private final WindowSpec mWindowSpec;

  /** The actual time interval over which we're aggregating. (Derived from mWindowSpec) */
  private final TimeSpan mTimeSpan;

  /**
   * The "width" of each bucket, in milliseconds. Specifies how we round the
   * true timestamps for events off, into the timestamps associated with
   * buckets in the time interval.
   */
  private final long mTimeModulus;

  /**
   * The maximum lateness (specified in milliseconds) we will tolerate for an
   * event.
   */
  private final long mSlackTime = 200; // TODO: Parameterize, and unify with HashJoinElt.

  /**
   * The set of aliased expressions describing the aggregation functions to run
   * over records we receive, and what alias to assign to their outputs.
   */
  private final List<AliasedExpr> mAggregateExprs;

  private final List<TypedField> mPropagateFields;

  /**
   * Map that returns a set of Bucket objects. Each bucket object
   * contains the state associated with a single aggregation function.
   * The key is a pair consisting of the timestamp (as a Long) and a HashedEvent:
   * an object that implements equals() and hashCode() based on a subset of the
   * fields of an EventWrapper.
   */
  private Map<Pair<Long, HashedEvent>, List<Bucket>> mBucketMap;

  /**
   * The same set of buckets as mBucketMap, organized as time-ordered lists
   * arranged by the group-by columns.
   */
  private Map<HashedEvent, List<Pair<Long, List<Bucket>>>> mBucketsByGroup;

  /**
   * Timestamp associated with the newest buckets in the pipeline.
   * This is used for auto-closing old windows when newer ones arrive.
   */
  private long mHeadBucketTime = 0;

  /**
   * Timestamp associated with the oldest bucket that can act as a window head
   * in the pipeline.
   */
  private long mTailBucketTime = 0;

  /** Timestamp of the most recent wakeup call enqueued. */
  private long mLastEnqueuedWakeup = 0;

  /**
   * SelectableQueue for the downstream timer element, which our eviction thread
   * enqueues into.
   */
  private SelectableQueue<Object> mTimerQueue = null;

  private EvictionThread mEvictionThread;

  public BucketedAggregationElement(FlowElementContext ctxt, AggregateNode aggregateNode) {
    super(ctxt, (Schema) aggregateNode.getAttr(PlanNode.OUTPUT_SCHEMA_ATTR));

    Configuration conf = aggregateNode.getConf();
    assert null != conf;
    mNumBuckets = conf.getInt(NUM_BUCKETS_KEY, DEFAULT_NUM_BUCKETS);
    mContinuousOutput = conf.getBoolean(CONTINUOUS_OUTPUT_KEY, DEFAULT_CONTINUOUS_OUTPUT);
    mMaxPriorEmitInterval = conf.getLong(MAX_PRIOR_EMIT_INTERVAL_KEY,
        DEFAULT_MAX_PRIOR_EMIT_INTERVAL);

    assert mMaxPriorEmitInterval > 0;
    assert mMaxPriorEmitInterval > mSlackTime;

    List<TypedField> groupByFields = aggregateNode.getGroupByFields();
    if (null == groupByFields) {
      mGroupByFields = Collections.emptyList();
    } else {
      mGroupByFields = groupByFields;
    }

    mAggregateExprs = aggregateNode.getAggregateExprs();
    assert mAggregateExprs != null;
    mPropagateFields = aggregateNode.getPropagateFields();

    Expr windowExpr = aggregateNode.getWindowExpr();
    assert windowExpr.isConstant();
    try {
      mWindowSpec = (WindowSpec) windowExpr.eval(new EmptyEventWrapper());
      assert mWindowSpec.getRangeSpec().isConstant();
      mTimeSpan = (TimeSpan) mWindowSpec.getRangeSpec().eval(new EmptyEventWrapper());
    } catch (IOException ioe) {
      // The only way this can be thrown is if the window expr isn't actually constant.
      // This should not happen due to the assert above..
      LOG.error("Got IOException when calculating window width: " + ioe);
      throw new RuntimeException(ioe);
    }

    mBucketMap = new HashMap<Pair<Long, HashedEvent>, List<Bucket>>(mNumBuckets);
    mBucketsByGroup = new HashMap<HashedEvent, List<Pair<Long, List<Bucket>>>>();

    // Calculate the width of each bucket.
    mTimeModulus = mTimeSpan.getWidth() / mNumBuckets;
    if (mTimeModulus * mNumBuckets != mTimeSpan.getWidth()) {
      LOG.warn("Aggregation time step does not cleanly divide the time interval; "
          + "results may be inaccurate. Set " + NUM_BUCKETS_KEY + " to a better divisor.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void open() throws IOException, InterruptedException {
    TimerFlowElemContext timerContext = (TimerFlowElemContext) getContext();
    // Start the auto-closing thread. Initialize the reference to the queue it populates
    // from our timer context.
    mTimerQueue = timerContext.getTimerQueue();
    mEvictionThread = new EvictionThread();
    mEvictionThread.start();
    super.open();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException, InterruptedException {
    // We've got no new elements coming in; expire all buckets immediately.
    LOG.debug("Immediately expiring all buckets to mHeadBucketTime=" + mHeadBucketTime);
    closeUntil(mHeadBucketTime, mHeadBucketTime, getContext());
    mEvictionThread.finish();
    mEvictionThread = null;
    super.close();
  }

  /**
   * Initialize the list of Bucket entries that are associated with a new
   * timestamp -&gt; bucket mapping. This is typically done just before inserting
   * a value in a new bucket at the head of a new time window.
   * @return the list of initialized Bucket objects for this time subrange.
   */
  private List<Bucket> initBuckets(Pair<Long, HashedEvent> bucketKey) {
    List<Bucket> newBuckets = new ArrayList<Bucket>(mAggregateExprs.size());
    for (int i = 0; i < mAggregateExprs.size(); i++) {
      // Put in a new bucket instance for each aggregation funtion we're going to run.
      newBuckets.add(new Bucket());
    }

    assert null == mBucketMap.get(bucketKey);
    mBucketMap.put(bucketKey, newBuckets);

    // Put this into the map organized by group, as well.
    // Get the set of (time, bucketlist) pairs for the group.
    List<Pair<Long, List<Bucket>>> bucketsByTime = mBucketsByGroup.get(bucketKey.getRight());
    if (null == bucketsByTime) {
      bucketsByTime = new LinkedList<Pair<Long, List<Bucket>>>();
      mBucketsByGroup.put(bucketKey.getRight(), bucketsByTime);
    }
    bucketsByTime.add(new Pair<Long, List<Bucket>>(bucketKey.getLeft(), newBuckets));

    // Return the initialized set of Bucket objects back to the caller.
    return newBuckets;
  }

  /**
   * @return a key into our group-by map that is composed of the bucket
   * timestamp for the event, and a HashedEvent that reads the fields
   * of the event necessary to group by those fields. If we are not grouping
   * by any fields, this component of the pair is null.
   */
  private Pair<Long, HashedEvent> getEventKey(EventWrapper e) {
    long eventTime = e.getEvent().getTimestamp();
    long remainder = eventTime % mTimeModulus;
    Long bucketTime;

    // If we're on an interval boundary (e.g., t=100) we go into that bucket.
    // If we're off-boundary (e.g., t=103), we go into the closest "previous" bucket (t=100).
    bucketTime = Long.valueOf(eventTime - remainder);
    HashedEvent hashedEvent = new HashedEvent(e, mGroupByFields);
    return new Pair<Long, HashedEvent>(bucketTime, hashedEvent);
  }

  /**
   * Given a set of time buckets associated with a given group,
   * iterate over the time buckets for a specific time interval,
   * for a particular aggregation function.
   */
  private static class BucketIterator<T> implements Iterator<Bucket<T>> {
    /**
     * Offset of the true Bucket object in the final List<Buckets> that
     * specifies buckest for each aggregation function we operate.
     */
    private final int mFunctionId;

    /** Lowest timestamped bucket we return. */
    private final long mLoTime;

    /** Highest timestamped bucket we return. */
    private final long mHiTime;

    /**
     * Iterator over the outer list. We require this iterator
     * to return values in order.
     */ 
    private final Iterator<Pair<Long, List<Bucket>>> mIterator;

    /** The next value we return. */
    private Bucket<T> mNextBucket;

    /** Set to true if prepBucket() has been called, but not next(). */
    private boolean mIsReady;

    /** The number of buckets in the time interval that were returned by this iterator. */
    private int mYieldCount;

    public BucketIterator(int functionId, long loTime, long hiTime,
        List<Pair<Long, List<Bucket>>> inputList) {
      mFunctionId = functionId;
      mLoTime = loTime;
      mHiTime = hiTime;
      mIterator = inputList.iterator();
      mYieldCount = 0;
    }

    /**
     * Scan ahead in the underlying iterator til we find the next element.
     * Set mNextBucket to the next value that next() should return, or null
     * if we cannot yield any more values.
     * Sets mIsReady to true.
     */
    private void prepBucket() {
      assert !mIsReady; // This should not be called twice in a row.

      mIsReady = true;
      mNextBucket = null;

      while (mIterator.hasNext()) {
        Pair<Long, List<Bucket>> nextPair = mIterator.next();
        long timestamp = nextPair.getLeft();
        if (timestamp > mLoTime && timestamp <= mHiTime) {
          // We found the next one to return.
          mNextBucket = nextPair.getRight().get(mFunctionId);
          return;
        }
      }
    }

    public Bucket<T> next() {
      if (!mIsReady) {
        prepBucket();
      }

      assert mIsReady;
      mIsReady = false;
      if (mNextBucket != null) {
        mYieldCount++;
      }
      return mNextBucket;
    }

    public boolean hasNext() {
      if (!mIsReady) {
        prepBucket();
      }

      assert mIsReady;
      return mNextBucket != null;
    }

    public void remove() {
      throw new RuntimeException("Not implemented.");
    }

    int getYieldCount() {
      return mYieldCount;
    }
  }

  /**
   * Close the window ending with the bucket for 'closeTime'.
   * Remove any buckets that are older than closeTime - aggregationIntervalWidth.
   * since they will no longer contribute to any open windows.
   */
  private void closeWindow(long closeTime, FlowElementContext context)
      throws IOException, InterruptedException {
    long loTime = closeTime - mTimeSpan.getWidth();
    Long closeBucketTimestamp = Long.valueOf(closeTime);

    LOG.debug("Closing window for range: " + loTime + " -> " + closeTime);

    // For each group, emit an output record containing the aggregate values over
    // the whole time window.
    for (Map.Entry<HashedEvent, List<Pair<Long, List<Bucket>>>> entry :
        mBucketsByGroup.entrySet()) {
      HashedEvent group = entry.getKey();

      // In non-continuous (demand-only) mode, check whether there's a bucket associated
      // with this window's closing time for this group.
      if (!mContinuousOutput &&
          mBucketMap.get(new Pair<Long, HashedEvent>(closeBucketTimestamp, group)) == null) {
        continue; // Nothing to do.
      }

      GenericData.Record record = new GenericData.Record(getOutputSchema());
      List<Pair<Long, List<Bucket>>> bucketsByTime = entry.getValue();

      int numBucketsInRangeForGroup = 0;
      // Execute each aggregation function over the applicable subset of buckets
      // in bucketsByTime.
      for (int i = 0; i < mAggregateExprs.size(); i++) {
        BucketIterator aggIterator = new BucketIterator(i, loTime, closeTime, bucketsByTime);
        AliasedExpr aliasExpr = mAggregateExprs.get(i);
        FnCallExpr fnCall = (FnCallExpr) aliasExpr.getExpr();
        Object result = fnCall.finishWindow(new IterableIterator(aggIterator));
        numBucketsInRangeForGroup += aggIterator.getYieldCount();
        record.put(aliasExpr.getAvroLabel(), result);
      }

      // If there are no buckets in bucketsByTime that are in our time range,
      // we should not emit anything for this group. Just silently continue.
      if (0 == numBucketsInRangeForGroup) {
        // Discard this output; we didn't actually calculate anything.
        continue;
      }

      // Copy the specified fields to propagate from the record used to define
      // the group, into the output record.
      EventWrapper groupWrapper = group.getEventWrapper();
      for (TypedField propagateField : mPropagateFields) {
        record.put(propagateField.getAvroName(), groupWrapper.getField(propagateField));
      }

      // Emit this as an output event!
      emitAvroRecord(record, groupWrapper.getEvent(), closeTime, context);
    }

    // Remove any buckets that are too old to be useful to any subsequent windows.
    // TODO(aaron): This is O(groups * num_buckets). We should actually use a TreeMap
    // instad of a list internally, so we can quickly cull the herd. That would be
    // O(groups * log(num_buckets)).
    Iterator<Map.Entry<HashedEvent, List<Pair<Long, List<Bucket>>>>> bucketsByGrpIter =
        mBucketsByGroup.entrySet().iterator();
    while (bucketsByGrpIter.hasNext()) {
      Map.Entry<HashedEvent, List<Pair<Long, List<Bucket>>>> entry = bucketsByGrpIter.next();
      HashedEvent group = entry.getKey();
      List<Pair<Long, List<Bucket>>> bucketsByTime = entry.getValue();
      Iterator<Pair<Long, List<Bucket>>> bucketsByTimeIter = bucketsByTime.iterator();
      while (bucketsByTimeIter.hasNext()) {
        Pair<Long, List<Bucket>> timedBucket = bucketsByTimeIter.next();
        Long timestamp = timedBucket.getLeft();
        if (timestamp.longValue() < loTime) {
          bucketsByTimeIter.remove(); // Remove from bucketsByTime list.
          Pair<HashedEvent, Long> key = new Pair<HashedEvent, Long>(group, timestamp);
          mBucketMap.remove(key); // Remove from mBucketMap.
        }
      }

      if (bucketsByTime.size() == 0) {
        // We've removed the last time bucket for a given group from mBucketsByGroup.
        // Remove the group from that map.
        bucketsByGrpIter.remove();
      }
    }
  }

  /**
   * Close all open windows up to and including the window that ends with the bucket
   * for time 'lastWindow'.
   */
  private void closeUntil(long curBucketTime, long lastWindow, FlowElementContext context)
      throws IOException, InterruptedException {

    LOG.debug("Close until: cur=" + curBucketTime + ", lastWindow=" + lastWindow
        + ", mTailBucketTime=" + mTailBucketTime + ", mTimeMod=" + mTimeModulus
        + ", mMaxPrior=" + mMaxPriorEmitInterval);
    if (lastWindow <= mTailBucketTime) {
      return; // We've already closed this window.
    }

    // If mHeadBucketTime is too far back from the current time,
    // do a mass expiration and throw out old data. closeTime is bounded by
    // mMaxPriorEmitInterval.
    for (long closeTime = Math.max(mTailBucketTime, curBucketTime - mMaxPriorEmitInterval);
        closeTime <= lastWindow; closeTime += mTimeModulus) {
      LOG.debug("Close window: closeTime=" + closeTime);
      closeWindow(closeTime, context);
    }

    mTailBucketTime = lastWindow + mTimeModulus;
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    Pair<Long, HashedEvent> bucketKey = getEventKey(e);

    long curBucketTime = bucketKey.getLeft();
    LOG.debug("Handling event time=" + curBucketTime);
    if (curBucketTime > mHeadBucketTime) {
      // We've just received an event that is newer than any others we've yet
      // received. This advances the sliding window to match this event's timestamp.
      // Emit any output groups that are older than this one by at least the
      // slack time interval.
      LOG.debug("New bucket: cur=" + curBucketTime + "; mHeadBucketTime=" + mHeadBucketTime);
      closeUntil(curBucketTime, curBucketTime - mSlackTime - mTimeModulus, getContext());
      // Since we've already handled these, remove their wake-up calls..
      mEvictionThread.discardUntil(mHeadBucketTime - mSlackTime);
      mHeadBucketTime = curBucketTime; // This insert advances our head bucket.
    } else if (curBucketTime < mHeadBucketTime - mMaxPriorEmitInterval) {
      // This event is too old -- ignore it.
      // TODO: Should this be mHeadBucketTiem - mSlackTime?
      LOG.debug("Dropping late event arriving at aggregator; HeadBucketTime=" + mHeadBucketTime
          + " and event is for bucket " + curBucketTime);
      return;
    }

    // Get the bucket for the (timestamp, group-by-fields) of this event.
    // Actually returns a list of Bucket objects, one per AggregateFunc to
    // execute.
    List<Bucket> buckets = mBucketMap.get(bucketKey);
    if (null == buckets) {
      // We're putting the first event into a new bucket.
      buckets = initBuckets(bucketKey);
    }

    // For each aggregation function we're performing, insert this event into
    // the bucket for the aggregate function.
    assert buckets.size() == mAggregateExprs.size();
    for (int i = 0; i < mAggregateExprs.size(); i++ ) {
      AliasedExpr aliasExpr = mAggregateExprs.get(i);
      Expr expr = aliasExpr.getExpr();
      assert expr instanceof FnCallExpr;
      FnCallExpr fnCall = (FnCallExpr) expr;
      Bucket bucket = buckets.get(i);
      fnCall.insertAggregate(e, bucket);
    }

    // Insert a callback into a queue to allow time to expire these windows.
    enqueueWakeup(curBucketTime);
  }

  /**
   * Enqueue a wakeup in the EvictionThread that closes the bucket with the
   * specified bucket timestamp.
   */
  private void enqueueWakeup(long bucketTime) {
    if (bucketTime <= mLastEnqueuedWakeup) {
      // We've already enqueued a wakeup to close this bucket.
      return;
    }

    long curTime = System.currentTimeMillis();
    long offset = mTimeModulus + mSlackTime;
    long closeTime = curTime + offset; // local time to close the bucket.
    LOG.debug("Insert wakeup call: " + bucketTime + " at time offset=" + offset);
    mEvictionThread.insert(new Pair<Long, Long>(closeTime, bucketTime));
    mLastEnqueuedWakeup = bucketTime;
  }

  /**
   * Thread that sends notices to our coprocessor FlowElement when it is time to
   * close old windows based on elapsed local time.
   */
  private class EvictionThread extends Thread {
    private final Logger LOG = LoggerFactory.getLogger(
        EvictionThread.class.getName());

    /**
     * Set to true when it's time for the thread to go home. The thread
     * actually exits after this flag is set to true and the incoming queue
     * is empty.
     */
    private boolean mIsFinished;

    /**
     * Priority queue (heap) of times when we should insert expiry-times in
     * the coprocessor FlowElement's input queue.
     *
     * <p>The queue holds tuples of two long values. The first is a local
     * time when this thread should wake up; this is what the queue is
     * ordered on. The latter is the window time that should be expired.</p>
     */
    private PriorityBlockingQueue<Pair<Long, Long>> mQueue;

    // Maximum queue length == number of open windows + the newly-opening window
    //     + the currently-closing window.
    final long mMaxQueueLen = 2 + (mSlackTime / mTimeModulus);

    public EvictionThread() {
      super("AggregatorEvictionThread");

      mQueue = new PriorityBlockingQueue<Pair<Long, Long>>((int) mMaxQueueLen,
          new PairLeftRightComparator<Long, Long>());
    }

    /**
     * Add a wake-up call to the queue.
     */
    public void insert(Pair<Long, Long> wakeUpCall) {
      synchronized (this) {
        assert mQueue.size() < mMaxQueueLen; // This operation should never block.
        mQueue.put(wakeUpCall);
        this.notify();
      }

      // Interrupt any wait that's going on, in case we are asleep and should
      // actually immediately service this wake-up call.
      this.interrupt();
    }

    /**
     * Discard all wakeup calls up to time 'minTime'.
     * minTime is a 'bucket time', not a 'local time'.
     */
    public void discardUntil(long minTime) {
      synchronized (this) {
        LOG.debug("discardUntil: " + minTime);
        Iterator<Pair<Long, Long>> iterator = mQueue.iterator();
        while (iterator.hasNext()) {
          Pair<Long, Long> wakeUpCall = iterator.next();
          if (wakeUpCall.getRight() < minTime) {
            LOG.debug("discard@ " + wakeUpCall); 
            iterator.remove();
          }
        }

        this.notify();
      }
    }

    /**
     * Set the finished flag to true; try to get the thread to stop as
     * quickly as possible.
     */
    public void finish() {
      synchronized (this) {
        this.mIsFinished = true;
        this.notify();
      }
      this.interrupt(); // Interrupt any current sleep.
    }

    /**
     * Main loop of the thread.
     * Continually sleeps until the next timer event is ready to occur.
     */
    public void run() {
      while (true) {
        Pair<Long, Long> wakeUpCall = null;
        long curTime;
        long nextWakeUp;

        synchronized (this) {
          while (mQueue.size() == 0) {
            try {
              if (this.mIsFinished) {
                // Parent is finished and we have drained our input queue. Go home.
                return;
              }
              this.wait();
            } catch (InterruptedException ie) {
              // Interrupted while waiting for another wake-up call to enter our queue.
              // Try again, if we're not already finished.
              continue;
            }
          }

          assert mQueue.size() > 0;
          wakeUpCall = mQueue.peek();
        }

        if (null == wakeUpCall) {
          continue;
        }

        curTime = System.currentTimeMillis();
        nextWakeUp = wakeUpCall.getLeft();
        if (nextWakeUp <= curTime) {
          // TODO(aaron): This section probably bears further deadlock analysis.
          // The put() into the timer queue can block (it has fixed length
          // LocalEnvironment.MAX_QUEUE_LEN) until the timer FE services its
          // existing list.
          // If we are interrupted doing this, it is because the main thread
          // has just inserted another wakeup call while we were blocking.
          // This thread's input queue must not block when being filled from
          // the main aggregation FE. I believe mMaxQueueLen should be sufficient
          // to guarantee this is the case, because before we call enqueueWakeup(),
          // we will have had to call closeUntil() in BucketedAggElem.takeEvent()
          // on enough windows to free up the slots in this queue.
          try {
            LOG.debug("Timer evicting at " + curTime + ": " + wakeUpCall);
            // Service this by injecting the getRight() into our outbound queue.
            mTimerQueue.put(new TimeoutEventWrapper(wakeUpCall.getRight()));
          } catch (InterruptedException ie) {
            // Not a problem. If we were interrupted doing the put into mTimerQueue,
            // then we'll service this again on the next go-around of the loop.
            // Just make sure we don't mark this as 'complete.'
            continue;
          }

          synchronized (this) {
            // Now actually remove this from the input queue.
            if (mQueue.peek() == wakeUpCall) {
              // O(1) fast path; no intervening push.
              mQueue.remove();
            } else {
              // intervening push of an earlier wakeup (?). Slow path.
              mQueue.remove(wakeUpCall);
            }
          }
        } else {
          // If we're down here, we need to sleep until it is the next wake-up time.
          long napTime = nextWakeUp - curTime;
          try {
            Thread.sleep(napTime);
          } catch (InterruptedException ie) {
            // We were awoken early... this is expected (there may have been a
            // new enqueue, etc).
          }
        }
      }
    }
  }

  /** EventWrapper used to deliver the expiry time payload to the TimeoutEvictionElement. */
  private static class TimeoutEventWrapper extends EmptyEventWrapper {
    /** The time window that should be expired. */
    private final Long mExpireWindow; 

    public TimeoutEventWrapper(Long expire) {
      mExpireWindow = expire;
    }

    @Override
    public Object getField(TypedField field) {
      return mExpireWindow;
    }
  }

  /**
   * Separate FlowElement that handles notifications from the EvictionThread; this
   * operates in the main thread, closing windows that cannot receive new events
   * because they are past the slack time interval.
   */
  public class TimeoutEvictionElement extends AvroOutputElementImpl {
    private final Logger LOG = LoggerFactory.getLogger(
        TimeoutEvictionElement.class.getName());

    private TimeoutEvictionElement(FlowElementContext ctxt, Schema outSchema) {
      super(ctxt, outSchema);
    }

    public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
      assert e instanceof TimeoutEventWrapper;
      Long expireTime = (Long) e.getField(null); // TimeoutEventWrapper returns a single Long val
      LOG.debug("Handling in eviction element - timeout to: " + expireTime);
      closeUntil(expireTime, expireTime, getContext());
    }
  }

  /**
   * Create a TimeoutEvictionElement coupled to this BucketedAggregationElement.
   */
  public TimeoutEvictionElement getTimeoutElement(FlowElementContext timeoutContext) {
    return this.new TimeoutEvictionElement(timeoutContext, getOutputSchema());
  }
}
