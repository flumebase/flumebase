// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

/**
 * <p>A timespan is internally represented entirely in arbitrary base units,
 * currently defined as milliseconds. A timespan is a pair of values, one defining
 * the bottom end of the interval, the other the top end.  The bottom interval
 * is always closed, the top end is always open: [bottom, top).</p>
 *
 * <p>Timespans may be absolute, or relative to the current time.</p>
 *
 * <p>Relative timespans have values for 'lo' and 'hi' given in seconds
 * relative to "now." Absolute timespans have values for 'lo' and 'hi'
 * relative to the Unix Epoch.</p>
 */
public class TimeSpan extends TimeSpanBase {

  public TimeSpan(long low, long high) {
    this(low, high, true);
  }

  public TimeSpan(long low, long high, boolean relative) {
    this.lo = low;
    this.hi = high;
    this.isRelative = relative;
  }

}
