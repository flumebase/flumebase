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

  public Timestamp(long millis, long nanos) {
    this.milliseconds = millis;
    this.nanos = nanos;
  }

  public Timestamp(java.sql.Timestamp sqlTimestamp) {
    this.milliseconds = sqlTimestamp.getTime();
    this.nanos = sqlTimestamp.getNanos();
  }

  @Override
  public int hashCode() {
    return ((int) (this.milliseconds & 0xFFFFFFFF)) ^ ((int) this.nanos * 37);
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
    return "" + milliseconds + "." + String.format("%06d", nanos);
  }
}
