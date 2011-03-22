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

import com.odiago.flumebase.parser.Expr;

/**
 * FlowElement that advances events whose fields when applied to the embedded
 * match expression, cause the match expr to evaluate to true.
 * TODO(aaron): Rewrite this to take expr opcodes, not an ast element.
 */
public class FilterElement extends FlowElementImpl {
  private Expr mFilterExpr;

  public FilterElement(FlowElementContext ctxt, Expr filterExpr) {
    super(ctxt);
    mFilterExpr = filterExpr;
  }


  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    if (Boolean.TRUE.equals(mFilterExpr.eval(e))) {
      emit(e);
    }
  }

  @Override
  public String toString() {
    return "Filter[filterExpr=\"" + mFilterExpr + "\"]";
  }
}
