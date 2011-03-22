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

import java.util.List;

import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.plan.FlowSpecification;
import com.odiago.flumebase.plan.PlanContext;

/**
 * Interface that identifies SQLStatement subclasses which are a source
 * of records of named fields.
 */
public abstract class RecordSource extends SQLStatement {

  /**
   * @return a SymbolTable that specifies the fields of each output
   * record of this source. The SymbolTable itself is memoized during
   * type checking.
   */
  public abstract SymbolTable getFieldSymbols();

  /**
   * @return a list of stream source names enclosed under this object.
   * For named streams and subselects, this is a singleton list; for 
   * joins, this may be two or more names.
   */
  public abstract List<String> getSourceNames();

  /**
   * @return the name of this stream. May be generated in the case of joins, etc.
   */
  public abstract String getSourceName();


  /**
   * For record sources with nested sub-sources, create the flow specification
   * for a nested source inside a new FlowSpecification DAG in a new PlanContext,
   * derived from the PlanContext specified as an argument.
   * @return the PlanContext containing the nested FlowSpecification.
   */
  protected PlanContext getSubPlan(SQLStatement subStmt, PlanContext planContext) {
    // Create an execution plan to build the source (it may be a single node
    // representing a Flume source or file, or it may be an entire DAG because
    // we use another SELECT statement as a source) inside a new context.
    PlanContext sourceInCtxt = new PlanContext(planContext);
    sourceInCtxt.setRoot(false);
    sourceInCtxt.setFlowSpec(new FlowSpecification(planContext.getConf()));
    return subStmt.createExecPlan(sourceInCtxt);
  }
}
