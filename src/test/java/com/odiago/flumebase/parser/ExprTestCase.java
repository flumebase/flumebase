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
