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

package com.odiago.flumebase.exec;

import java.io.IOException;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.parser.TypedField;

/**
 * An EventWrapper instance that wraps no actual data; it returns null
 * for all fields. It is intended to be passed to Expr.eval() methods
 * which are guaranteed to be constant, and should not depend on any
 * per-record state. The getField() method throws IOException for all
 * requests.
 */
public class EmptyEventWrapper extends EventWrapper {
  private Event mEvent;

  public EmptyEventWrapper() {
  }

  @Override
  public void reset(Event e) {
    mEvent = e;
  }

  @Override
  public Object getField(TypedField field) throws IOException {
    throw new IOException("EmptyEventWrapper cannot access field: " + field);
  }

  @Override
  public Event getEvent() {
    return mEvent;
  }

  @Override
  public String getAttr(String attrName) {
    return null;
  }

  @Override
  public String getEventText() {
    return "(empty)";
  }
}
