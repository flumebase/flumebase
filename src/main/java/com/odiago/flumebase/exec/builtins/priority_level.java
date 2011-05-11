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

package com.odiago.flumebase.exec.builtins;

import java.util.Collections;
import java.util.List;

import com.cloudera.flume.core.Event;

import com.odiago.flumebase.exec.EventWrapper;

import com.odiago.flumebase.lang.ScalarFunc;
import com.odiago.flumebase.lang.Type;

/**
 * Return the priority field of the current event as an INT.
 */
public class priority_level extends ScalarFunc {
  @Override
  public Type getReturnType() {
    return Type.getPrimitive(Type.TypeName.INT);
  }

  @Override
  public Object eval(EventWrapper event, Object... args) {
    Event e = event.getEvent();
    return Integer.valueOf(e.getPriority().ordinal());
  }

  @Override
  public List<Type> getArgumentTypes() {
    return Collections.emptyList();
  }
}
