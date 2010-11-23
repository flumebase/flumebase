// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

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
   * Given a set of fields required as output from a layer of a logical plan,
   * return an Avro schema that encompasses all these fields with the correct
   * Avro types for the fields based on the symbol table entry for each field.
   * @param requiredFields the field names to be included in this schema.
   * @param symTab the symbol table populated with definitions for all these
   * fields.
   */
  protected static Schema createFieldSchema(Set<String> requiredFields, SymbolTable symTab) {
    List<Schema.Field> avroFields = new ArrayList<Schema.Field>();
    Schema record = Schema.createRecord(AVRO_RECORD_NAME, null, null, false);
    for (String fieldName : requiredFields) {
      Symbol sym = symTab.resolve(fieldName);
      Type t = sym.getType();
      Schema fieldSchema = t.getAvroSchema();
      Schema.Field field = new Schema.Field(fieldName, fieldSchema, null, null);
      avroFields.add(field);
    }
    record.setFields(avroFields);
    return record;
  }
}

