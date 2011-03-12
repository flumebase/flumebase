// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

/**
 * A TimeWidth defines the human-friendly multiplier associated with a value
 * to convert it into the internal time units used by the windowing engine.
 * <p>e.g., 24 * TimeWidth.Hours = 1 TimeWidth.Days</p>
 */
public enum TimeWidth {
  BaseUnits(1), // Internally, all windowing is in terms of base units.
  Milliseconds(1), // BaseUnits are equivalent to milliseconds.
  Seconds(1000L),
  Minutes(60 * 1000L),
  Hours(60 * 60 * 1000L),
  Days(24 * 60 * 60 * 1000L),
  Weeks(7 * 24 * 60 * 60 * 1000L),
  Months(30 * 24 * 60 * 60 * 1000L), // TODO(aaron): This needs to be more flexible than this..
  Years(365 * 24 * 60 * 60 * 1000L);

  /** How much to multiply a time unit by to get it into base units. */
  private final long mMultiplier;

  private TimeWidth(long multiplier) {
    mMultiplier = multiplier;
  }

  public long getMultiplier() {
    return mMultiplier;
  }
}
