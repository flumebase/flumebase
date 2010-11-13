// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.parser;

/**
 * SELECT statement.
 */
public class SelectStmt extends SQLStatement {
  private FieldList mFields;
  // Source stream for the FROM clause. must be a LiteralSource or a SelectStmt.
  // (That fact is proven by a TypeChecker visitor.)
  private SQLStatement mSource;
  private WhereConditions mWhere;

  public SelectStmt(FieldList fields, SQLStatement source, WhereConditions where) {
    mFields = fields;
    mSource = source;
    mWhere = where;
  }

  public FieldList getFields() {
    return mFields;
  }

  public SQLStatement getSource() {
    return mSource;
  }

  public WhereConditions getWhereConditions() {
    return mWhere;
  }

  @Override
  public void format(StringBuilder sb, int depth) {
    pad(sb, depth);
    sb.append("SELECT");
    sb.append("\n");
    pad(sb, depth);
    sb.append("fields:\n");
    if (mFields.isAllFields()) {
      pad(sb, depth + 1);
      sb.append("(all)\n");
    } else {
      for (String fieldName : mFields) {
        pad(sb, depth + 1);
        sb.append(fieldName);
        sb.append("\n");
      }
    }
    pad(sb, depth);
    sb.append("FROM:\n");
    mSource.format(sb, depth + 1);
    if (null != mWhere) {
      pad(sb, depth);
      sb.append("WHERE:\n");
      pad(sb, depth + 1);
      sb.append(mWhere.getText());
      sb.append("\n");
    }
  }
}

