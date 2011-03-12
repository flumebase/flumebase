// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import java.util.ArrayList;

import org.testng.annotations.Test;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.ParsingEventWrapper;

import com.odiago.flumebase.io.DelimitedEventParser;

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
