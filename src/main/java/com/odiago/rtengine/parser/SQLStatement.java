// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;

import com.odiago.rtengine.lang.Type;
import com.odiago.rtengine.lang.VisitException;
import com.odiago.rtengine.lang.Visitor;

import com.odiago.rtengine.plan.PlanContext;

/**
 * Abstract base class for statements in the SQL statement AST
 */
public abstract class SQLStatement {
  /**
   * The Avro record type used to communicate any entities from one stage of a
   * logical plan pipeline to another is always called the same thing, to make
   * projection easier.
   */
  public static final String AVRO_RECORD_NAME = "FlowRecord";

  /**
   * Format the contents of this AST into the provided StringBuilder,
   * starting with indentation depth 'depth'.
   */
  public abstract void format(StringBuilder sb, int depth);
  
  public void format(StringBuilder sb) {
    format(sb, 0);
  }

  /**
   * Format the contents of this AST to a string.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    format(sb);
    return sb.toString();
  }

  /**
   * Facilitate the visitor pattern over this AST.
   */
  public void accept(Visitor v) throws VisitException {
    v.visit(this);
  }

  /**
   * Add indentation to the current line of the string builder
   * for the current AST depth.
   */
  protected void pad(StringBuilder sb, int depth) {
    for (int i = 0; i < depth; i++) {
      sb.append("  ");
    }
  }

  /**
   * Transform this AST element into a set of DAG elements for the execution plan.
   */
  public PlanContext createExecPlan(PlanContext planContext) {
    throw new RuntimeException("This node type cannot be incorporated into an execution plan.");
  }

  /**
   * @return a list of fields representing the unique entries in inFields.
   * Callers of this method may depend on the order of outFields being the same as
   * the order of inFields (the second, third, etc. instances of a given field
   * are the omitted ones).
   */
  protected static List<TypedField> distinctFields(List<TypedField> inFields) {
    List<TypedField> out = new ArrayList<TypedField>();
    Set<String> fieldNames = new HashSet<String>();

    for (TypedField field : inFields) {
      String fieldName = field.getName();
      if (fieldNames.contains(fieldName)) {
        continue;
      }

      fieldNames.add(fieldName);
      out.add(field);
    }

    return out;
  }

  /**
   * Given a set of fields required as output from a layer of a logical plan,
   * return an Avro schema that encompasses all these fields with the correct
   * Avro types for the fields based on the symbol table entry for each field.
   * @param requiredFields the field names and types to be included in this schema.
   */
  protected static Schema createFieldSchema(Collection<TypedField> requiredFields) {
    List<Schema.Field> avroFields = new ArrayList<Schema.Field>();
    Schema record = Schema.createRecord(AVRO_RECORD_NAME, null, null, false);
    for (TypedField field : requiredFields) {
      String fieldName = field.getAvroName();
      Type t = field.getType();
      Schema fieldSchema = t.getAvroSchema();
      Schema.Field avroField = new Schema.Field(fieldName, fieldSchema, null, null);
      avroFields.add(avroField);
    }
    record.setFields(avroFields);
    return record;
  }
}

