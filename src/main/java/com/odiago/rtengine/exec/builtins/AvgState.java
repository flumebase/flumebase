// (c) Copyright 2011 Odiago, Inc.

package com.odiago.rtengine.exec.builtins;

/**
 * State for the avg() aggregate function.
 */
class AvgState {
  int mCount;
  Number mSum;

  AvgState() {
    mCount = 0;
    mSum = Integer.valueOf(0);
  }

  AvgState(int count, Number sum) {
    mCount = count;
    mSum = sum;
  }
}
