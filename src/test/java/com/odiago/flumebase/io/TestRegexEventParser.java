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

package com.odiago.flumebase.io;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.util.Utf8;

import static org.testng.AssertJUnit.*;
import org.testng.annotations.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

import com.odiago.flumebase.lang.Type;

public class TestRegexEventParser {
  @Test
  public void testReadFields() throws ColumnParseException, IOException {
    // Check that we can read a record with two string columns and an int column
    // with the regex /([^=]*)=(\\d*),(.*)/

    Map<String, String> properties = new HashMap<String, String>();
    properties.put("regex", "([^=]*)=(\\d*),(.*)");

    Event event = new EventImpl("foo=42,this is a lovely record".getBytes());
    EventParser parser = new RegexEventParser(properties);
    parser.reset(event);

    CharSequence field1 = (CharSequence) parser.getColumn(0,
        Type.getPrimitive(Type.TypeName.STRING));
    assertEquals(new Utf8("foo"), field1);

    Integer field2 = (Integer) parser.getColumn(1, Type.getPrimitive(Type.TypeName.INT));
    assertEquals(Integer.valueOf(42), field2);

    CharSequence field3 = (CharSequence) parser.getColumn(2,
        Type.getPrimitive(Type.TypeName.STRING));
    assertEquals(new Utf8("this is a lovely record"), field3);
  }
}
