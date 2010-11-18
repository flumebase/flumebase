// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.TypeChecker;

import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FieldList;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;

import com.odiago.rtengine.util.Stack;

/**
 * Run a type-checker over all elements of the AST.
 */
public class TypeChecker extends Visitor {

  /** Stack containing the symbol table for the current visit context. */
  private Stack<SymbolTable> mSymTableContext;

  public TypeChecker(SymbolTable rootSymbolTable) {
    mSymTableContext = new Stack<SymbolTable>();
    mSymTableContext.push(rootSymbolTable);
  }

  @Override
  protected void visit(CreateStreamStmt s) throws VisitException {
    // Nothing to do.
  }

  @Override
  protected void visit(LiteralSource s) throws VisitException {
    SymbolTable symtab = mSymTableContext.top();

    String name = s.getName();
    Symbol symbol = symtab.resolve(name);
    if (null == symbol) {
      throw new TypeCheckException("No such identifier: " + name);
    } else if (symbol.getType() != Symbol.SymbolType.STREAM) {
      throw new TypeCheckException("Identifier " + name + " is not a stream (type="
          + symbol.getType());
    }
  }

  @Override
  protected void visit(SelectStmt s) throws VisitException {
    FieldList fields = s.getFields();
    if (!fields.isAllFields()) {
      for (String fieldName : fields) {
        // TODO(aaron) Check that this field is defined for one of the inputs.
      }
    }

    SQLStatement source = s.getSource();
    if (source instanceof SelectStmt || source instanceof LiteralSource) {
      source.accept(this);
    } else {
      throw new TypeCheckException("Invalid source in FROM clause; this must be "
          + "an identifier, or a SELECT statement. Got " + source.getClass().getName());
    }

    // TODO(aaron): Check the where clause for validity if it's nonnull.
    // TODO(aaron): Create a new symbol table containing the fields defined in this
    // SELECT statement and push that on top of symTableContext before checking the
    // WHERE clause. Don't forget to pop it when we're done.
  }

  @Override
  protected void visit(ExplainStmt s) throws VisitException {
    s.getChildStmt().accept(this);
  }
}
