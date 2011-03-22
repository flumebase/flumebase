/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE.txt file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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
