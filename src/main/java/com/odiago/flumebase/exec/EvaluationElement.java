// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.exec;

import java.io.IOException;

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
      Object result = expr.eval(e);
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
