// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;

import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.parser.TypedField;

/**
 * Container for state associated with the plan-formation process
 * when operating over the statement AST.
 */
public class PlanContext {
  /** The string builder for messages to the user generated during planning. */
  private StringBuilder mMsgBuilder;

  /** The DAG we are forming to plan this query. */
  private FlowSpecification mFlowSpec;

  /** The symbol table for the current context. */
  private SymbolTable mSymTable;

  /** The schema describing the fields of a stage of processing.
   * This is the input schema when a PlanContext is used as the argument to
   * createExecPlan(); as a return value, this describes the output schema.
   */
  private Schema mSchema;


  /**
   * The list of field names and types provided as output from the plan of a source statement.
   * i.e., the set of fields returned by a nested SELECT or named source.
   */
  private List<TypedField> mOutFields;

  /**
   * Set to true if we should explain the plan after it is all fully-processed,
   * but not actually execute it.
   */
  private boolean mIsExplain;

  /**
   * True if we are building the 'root' FlowSpecification; false if we are
   * building a FlowSpecification intended to be incorporated into a larger
   * FlowSpecification higher up in the AST.
   */
  private boolean mIsRoot;

  /**
   * The user's configuration.
   */
  private Configuration mConf;

  public PlanContext() {
    mConf = new Configuration();
    mMsgBuilder = new StringBuilder();
    mFlowSpec = new FlowSpecification(mConf);
    mSymTable = new HashSymbolTable();
    mIsRoot = true;
    mSchema = null;
    mIsExplain = false;
    mOutFields = new ArrayList<TypedField>();
  }

  public PlanContext(PlanContext other) {
    mConf = other.mConf;
    mMsgBuilder = other.mMsgBuilder;
    mFlowSpec = other.mFlowSpec;
    mSymTable = other.mSymTable;
    mIsRoot = other.mIsRoot;
    mSchema = other.mSchema;
    mIsExplain = other.mIsExplain;
    mOutFields = other.mOutFields;
  }

  public Configuration getConf() {
    return mConf;
  }

  public void setConf(Configuration conf) {
    mConf = conf;
    mFlowSpec.setConf(mConf);
  }

  public List<TypedField> getOutFields() {
    return mOutFields;
  }

  public void setOutFields(Collection<TypedField> outFields) {
    this.mOutFields = new ArrayList<TypedField>(outFields);
  }

  public boolean isRoot() {
    return mIsRoot;
  }

  public void setRoot(boolean root) {
    mIsRoot = root;
  }

  public StringBuilder getMsgBuilder() {
    return mMsgBuilder;
  }

  public FlowSpecification getFlowSpec() {
    return mFlowSpec;
  }

  public void setMsgBuilder(StringBuilder sb) {
    mMsgBuilder = sb;
  }

  public void setFlowSpec(FlowSpecification flow) {
    mFlowSpec = flow;
  }

  public void setSymbolTable(SymbolTable t) {
    mSymTable = t;
  }

  public SymbolTable getSymbolTable() {
    return mSymTable;
  }

  public Schema getSchema() {
    return mSchema;
  }

  public void setSchema(Schema s) {
    mSchema = s;
  }

  public void setExplain(boolean explain) {
    mIsExplain = explain;
  }

  public boolean isExplain() {
    return mIsExplain;
  }
}
