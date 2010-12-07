// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;

import org.junit.Test;

import com.odiago.rtengine.exec.EventWrapper;
import com.odiago.rtengine.exec.ParsingEventWrapper;

import com.odiago.rtengine.io.DelimitedEventParser;

public class ExprTestCase {
  /** Create an event wrapper for an empty record. */
  protected EventWrapper getEmptyEventWrapper() {
    return new ParsingEventWrapper(new DelimitedEventParser(), new ArrayList<String>());
  }


  // Added so surefire doesn't complain.
  @Test
  public void ignoredTest() {
  }
}
