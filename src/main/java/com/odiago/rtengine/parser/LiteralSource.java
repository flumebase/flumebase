// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.StreamSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.plan.NamedSourceNode;
import com.odiago.rtengine.plan.PlanContext;
import com.odiago.rtengine.plan.PlanNode;

/**
 * Specify a source for the FROM clause of a SELECT statement that
 * references the literal name of a stream.
 *
 * A LiteralSource is not an executable SQLStatement, but it shares
 * the common hierarchy.
 */
public class LiteralSource extends SQLStatement {
  private String mSourceName;

  public LiteralSource(String name) {
    mSourceName = name;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("Literal source: ");
    sb.append(mSourceName);
    sb.append("\n");
  }


  public String getName() {
    return mSourceName;
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
    SymbolTable outTable = new HashSymbolTable(inTable);
    outContext.setSymbolTable(outTable);

    // Guaranteed to be a non-null StreamSymbol by the typechecker.
    StreamSymbol streamSym = (StreamSymbol) inTable.resolve(mSourceName);
    List<TypedField> fields = streamSym.getFields();
    List<String> fieldNames = new ArrayList<String>();
    for (TypedField field : fields) {
      String fieldName = field.getName();
      if (!fieldNames.contains(fieldName)) {
        outTable.addSymbol(new Symbol(field.getName(), field.getType()));
        fieldNames.add(fieldName);
      }
    }

    PlanNode node = new NamedSourceNode(mSourceName, fields);
    planContext.getFlowSpec().addRoot(node);

    // Create an Avro output schema for this node, specifying all the fields
    // we can emit.
    List<TypedField> outFields = new ArrayList<TypedField>();
    for (String fieldName : fieldNames) {
      Symbol sym = outTable.resolve(fieldName);
      outFields.add(new TypedField(fieldName, sym.getType()));
    }
    Schema outSchema = createFieldSchema(outFields);
    outContext.setSchema(outSchema);
    outContext.setOutFields(outFields);
    node.setAttr(PlanNode.OUTPUT_SCHEMA_ATTR, outSchema);

    return outContext;
  }
}

