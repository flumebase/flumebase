// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

/**
 * A Timestamp defines a point in time with nanosecond precision.
 * Timestamps are defined as a number of milliseconds and nanoseconds
 * since the UNIX Epoch (1/1/1970).
 */
public class Timestamp extends TimestampBase {

  public Timestamp(long millis) {
    this(millis, 0);
  }

  public Timestamp(long millis, int nanos) {
    this.milliseconds = millis;
    this.nanos = nanos;
  }

  public Timestamp(java.sql.Timestamp sqlTimestamp) {
    this.milliseconds = sqlTimestamp.getTime();
    this.nanos = sqlTimestamp.getNanos();
  }

}
