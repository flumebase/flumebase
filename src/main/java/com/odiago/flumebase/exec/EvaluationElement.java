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

import java.math.BigDecimal;

import java.util.List;

import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;

import com.odiago.flumebase.parser.AliasedExpr;
import com.odiago.flumebase.parser.Expr;
import com.odiago.flumebase.parser.TypedField;

/**
 * Evaluate a set of expressions giving computed output field values.
 */
public class EvaluationElement extends AvroOutputElementImpl {

  /** The expressions to evaluate. */
  private List<AliasedExpr> mExprs;

  /** Additional fields to propagate forward. */
  private List<TypedField> mPropagateFields;

  public EvaluationElement(FlowElementContext ctxt, List<AliasedExpr> exprs,
      List<TypedField> propagateFields, Schema outputSchema) {
    super(ctxt, outputSchema);
    mExprs = exprs;
    mPropagateFields = propagateFields;
  }

  @Override
  public void takeEvent(EventWrapper e) throws IOException, InterruptedException {
    GenericData.Record record = new GenericData.Record(getOutputSchema());

    // Evaluate all our input expressions, left-to-right, and emit
    // their results into the output record.
    for (AliasedExpr aliasedExpr : mExprs) {
      Expr expr = aliasedExpr.getExpr();
      Object result = nativeToAvro(expr.eval(e), expr.getResolvedType());
      String fieldName = aliasedExpr.getAvroLabel();
      record.put(fieldName, result);
    }

    // Now add to our output record, any fields that we can pull in directly from
    // the propagation layer.
    for (TypedField field : mPropagateFields) {
      String fieldName = field.getAvroName();
      record.put(fieldName, e.getField(field));
    }

    emitAvroRecord(record, e.getEvent());
  }
}
