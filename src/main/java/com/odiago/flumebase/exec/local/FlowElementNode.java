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

import com.odiago.flumebase.exec.FlowElement;

import com.odiago.flumebase.util.DAGNode;

/**
 * A DAGNode that holds a FlowElement in a local flow.
 */
public class FlowElementNode extends DAGNode<FlowElementNode> {
  private FlowElement mElem;

  public FlowElementNode(FlowElement fe) {
    super(0); // don't worry about node ids in this graph.
    mElem = fe;
  }

  public FlowElement getFlowElement() {
    return mElem;
  }

  @Override
  protected void formatParams(StringBuilder sb) {
    sb.append(mElem.toString());
  }

}
