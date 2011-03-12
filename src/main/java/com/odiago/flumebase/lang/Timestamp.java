// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.lang;

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

  @Override
  public int hashCode() {
    return ((int) (this.milliseconds & 0xFFFFFFFF)) ^ (this.nanos * 37);
  }

  @Override
  public boolean equals(Object otherObj) {
    if (otherObj == this) {
      return true;
    } else if (null == otherObj) {
      return false;
    } else if (!otherObj.getClass().equals(getClass())) {
      return false;
    }

    Timestamp other = (Timestamp) otherObj;
    return other.milliseconds == milliseconds && other.nanos == nanos;
  }

  @Override
  public String toString() {
    return "" + milliseconds + "." + nanos;
  }
}
