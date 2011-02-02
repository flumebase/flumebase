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
  public static List<TypedField> distinctFields(List<TypedField> inFields) {
    List<TypedField> out = new ArrayList<TypedField>();
    Set<String> fieldNames = new HashSet<String>();

    for (TypedField field : inFields) {
      String fieldName = field.getAvroName();
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
  public static Schema createFieldSchema(Collection<TypedField> requiredFields) {
    return createFieldSchema(requiredFields, AVRO_RECORD_NAME);
  }

  public static Schema createFieldSchema(Collection<TypedField> requiredFields,
      String recordName) {
   
    List<Schema.Field> avroFields = new ArrayList<Schema.Field>();
    Schema record = Schema.createRecord(recordName, null, null, false);
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

  /**
   * Given the name of a field, return a field name that is safe to use as an Avro
   * field name that is as close to the original name as possible.
   */
  protected static String avroSafeName(String name) {
    boolean isFirst = true;
    StringBuilder sb = new StringBuilder();
    for (char c : name.toCharArray()) {
      if (isFirst) {
        // First can only be [A-Za-z_]
        if (Character.isLetter(c) || c == '_') {
          sb.append(c);
        } else if (Character.isDigit(c)) {
          // Fields like '3xx' turn into '_3xx'.
          sb.append('_');
          sb.append(c);
        } else {
          // Turn anything else into a '_'.
          sb.append('_');
        }
        
        isFirst = false;
      } else {
        if (Character.isLetter(c) || c == '_' || Character.isDigit(c)) {
          sb.append(c);
        } else if (c == ' ' || c == '\t' || c == '(' || c == ')') {
          // Ignore whitespace characters or parentheses (e.g., because this
          // is the result of an expression).
        } else {
          // Unsure what to do with a special character (e.g., punctuation).
          // Turn it into an underscore.
          sb.append('_');
        }
      }
    }

    return sb.toString();
  }
}

