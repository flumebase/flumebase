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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.flumebase.exec.AliasSymbol;
import com.odiago.flumebase.exec.AssignedSymbol;
import com.odiago.flumebase.exec.HashSymbolTable;
import com.odiago.flumebase.exec.StreamSymbol;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.plan.NamedSourceNode;
import com.odiago.flumebase.plan.PlanContext;
import com.odiago.flumebase.plan.PlanNode;

import com.odiago.flumebase.util.Ref;

/**
 * Specify a source for the FROM clause of a SELECT statement that
 * references the literal name of a stream.
 *
 * A LiteralSource is not an executable SQLStatement, but it shares
 * the common hierarchy.
 */
public class LiteralSource extends RecordSource {
  private static final Logger LOG = LoggerFactory.getLogger(
      LiteralSource.class.getName());

  /** The actual name of the source stream. */
  private String mSourceName;

  /** A user-specified alias to identify fields of this stream in expressions. */
  private String mAlias;

  /** SymbolTable containing all the fields of this source with their assigned
   * labels.*/
  private SymbolTable mSymbols;

  public LiteralSource(String name) {
    mSourceName = name;
  }

  public void setAlias(String alias) {
    mAlias = alias;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("Literal source: name=");
    sb.append(mSourceName);
    if (null != mAlias) {
      sb.append(", alias=");
      sb.append(mAlias);
    }
    sb.append("\n");
  }


  /**
   * Returns the actual name of the source object.
   */
  public String getName() {
    return mSourceName;
  }

  /**
   * Returns the user-specified alias for this object.
   */
  public String getAlias() {
    return mAlias;
  }

  /** {@inheritDoc} */
  @Override
  public List<String> getSourceNames() {
    return Collections.singletonList(getSourceName());
  }

  /** {@inheritDoc} */
  @Override
  public String getSourceName() {
    if (null != mAlias) {
      return mAlias;
    } else {
      return mSourceName;
    }
  }

  /**
   * Given an input symbol table that defines this source, return a
   * SymbolTable that also includes the fields of this source. Memoizes the
   * created symbol table for later.
   *
   * <p>Modifies nextFieldId to contain the next id after applying ids to all
   * fields of this stream.</p>
   */
  public SymbolTable getFieldsSymbolTable(SymbolTable inTable, Ref<Integer> nextFieldId) {
    int nextId = nextFieldId.item.intValue();
    SymbolTable outTable = new HashSymbolTable(inTable);

    // Guaranteed non-null by our typechecker.
    StreamSymbol streamSym = (StreamSymbol) inTable.resolve(getName()).resolveAliases();

    String streamAlias = getAlias();
    if (null == streamAlias) {
      streamAlias = getName();
    }

    for (TypedField field : streamSym.getFields()) {
      String fieldName = field.getUserAlias();

      // This field is available as 'streamName.fieldName'.
      String fullName = streamAlias + "." + fieldName;
      AssignedSymbol sym = new AssignedSymbol(fullName, field.getType(), "__f_" + nextId + "_");
      sym.setParentName(streamAlias);
      nextId++;
      outTable.addSymbol(sym);

      // And also as an alias of just the fieldName.
      outTable.addSymbol(new AliasSymbol(fieldName, sym));
    }

    nextFieldId.item = Integer.valueOf(nextId);
    mSymbols = outTable;
    return outTable;
  }

  /** {@inheritDoc} */
  @Override
  public SymbolTable getFieldSymbols() {
    return mSymbols;
  }

  @Override
  public PlanContext createExecPlan(PlanContext planContext) {
    // The execution plan for a literal source is to just open the resouce
    // specified by this abstract source, by looking up its parameters in
    // the symbol table at plan resolution time.

    // The output PlanContext contains a new symbol table defining the fields
    // of this source.

    PlanContext outContext = new PlanContext(planContext);
    SymbolTable inTable = planContext.getSymbolTable();
    SymbolTable outTable = mSymbols;
    outContext.setSymbolTable(outTable);

    // streamSym is guaranteed to be a non-null StreamSymbol by the typechecker.
    StreamSymbol streamSym = (StreamSymbol) inTable.resolve(mSourceName).resolveAliases();
    List<TypedField> fields = streamSym.getFields();
    List<String> fieldNames = new ArrayList<String>();
    for (TypedField field : fields) {
      String fieldName = field.getAvroName();
      if (!fieldNames.contains(fieldName)) {
        fieldNames.add(fieldName);
      }
    }

    // Create an Avro output schema for this node, specifying all the fields
    // we can emit.  Use our internal symbol (mSymbols a.k.a. outTable) to
    // create more precise TypedFields that use the proper avro names.
    List<TypedField> outFields = new ArrayList<TypedField>();
    for (String fieldName : fieldNames) {
      AssignedSymbol sym = (AssignedSymbol) outTable.resolve(fieldName).resolveAliases();
      outFields.add(new TypedField(fieldName, sym.getType(), sym.getAssignedName(), fieldName));
    }

    PlanNode node = new NamedSourceNode(mSourceName, outFields);
    planContext.getFlowSpec().addRoot(node);
    Schema outSchema = createFieldSchema(outFields);
    outContext.setSchema(outSchema);
    outContext.setOutFields(outFields);
    node.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outSchema);

    return outContext;
  }
}

