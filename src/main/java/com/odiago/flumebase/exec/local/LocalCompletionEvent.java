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

package com.odiago.flumebase.exec.local;

/**
 * Indicates that a FlowElement in a LocalFlow has completed its processing,
 * and the LocalEnvironment should remove it from service to free resources;
 * downstream element(s) should be notified.
 */
public class LocalCompletionEvent {

  /** The completed FE's context. */
  private final LocalContext mCompleteContext;

  public LocalCompletionEvent(LocalContext context) {
    mCompleteContext = context;
  }

  public LocalContext getContext() {
    return mCompleteContext;
  }
}

