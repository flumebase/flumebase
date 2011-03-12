// (c) Copyright 2010 Odiago, Inc.

package com.odiago.flumebase.parser;

import java.util.Collections;
import java.util.List;

import com.odiago.flumebase.exec.EventWrapper;
import com.odiago.flumebase.exec.SymbolTable;

import com.odiago.flumebase.lang.StreamType;
import com.odiago.flumebase.lang.Type;

/**
 * Pseudo-expression containing just a '*' which represents
 * all fields. Only valid under specific circumstances:
 * being in a top-level SELECT expression, or in a COUNT(*).
 */
public class AllFieldsExpr extends Expr {

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("AllFields (*)\n");
  }

  @Override
  public String toStringOneLine() {
    return "*";
  }

  @Override
  public Type getType(SymbolTable symTab) {
    // TODO(aaron): Return the stream type associated with the current stream.
    return StreamType.getEmptyStreamType();
  }

  @Override
  public List<TypedField> getRequiredFields(SymbolTable symTab) {
    // We actually lack the context inside this expression to know what field
    // names we require. This is handled in the select statement directly.
    return Collections.emptyList();
  }

  @Override
  public Object eval(EventWrapper e) {
    // This object should be culled from lists of expressions before the evaluation step.
    throw new RuntimeException("Cannot call eval() on AllFieldsExpr");
  }

  @Override
  public Type getResolvedType() {
    throw new RuntimeException("Cannot call getResolvedType on AllFieldsExpr");
  }

  @Override
  public boolean isConstant() {
    return false;
  }
}
