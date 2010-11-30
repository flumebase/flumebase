// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.StreamSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.TypeChecker;

import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.DescribeStmt;
import com.odiago.rtengine.parser.DropStmt;
import com.odiago.rtengine.parser.EntityTarget;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FieldList;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.ShowStmt;
import com.odiago.rtengine.parser.TypedField;

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
  protected void visit(DropStmt s) throws VisitException {
    // Check that the DROP ____ type matches the type of the object to be dropped.
    SymbolTable symtab = mSymTableContext.top();
    String name = s.getName();
    Symbol sym = symtab.resolve(name);
    if (null == sym) {
      throw new TypeCheckException("No such object at top level: " + name);
    }
    EntityTarget targetType = s.getType();
    Type.TypeName symType = sym.getType().getTypeName();
    // Check that the DROP ___ type matches the symbol type.
    if (EntityTarget.Stream.equals(targetType)
        && !Type.TypeName.STREAM.equals(symType)) {
      throw new TypeCheckException("Entity " + name + " has incorrect type: " + symType);
    } else if (EntityTarget.Flow.equals(targetType)
        && !Type.TypeName.FLOW.equals(symType)) {
      throw new TypeCheckException("Entity " + name + " has incorrect type: " + symType);
    }
  }

  @Override
  protected void visit(ShowStmt s) throws VisitException {
    // Nothing to do.
  }

  @Override
  protected void visit(LiteralSource s) throws VisitException {
    SymbolTable symtab = mSymTableContext.top();

    String name = s.getName();
    Symbol symbol = symtab.resolve(name);
    if (null == symbol) {
      throw new TypeCheckException("No such identifier: " + name);
    } else if (symbol.getType().getTypeName() != Type.TypeName.STREAM) {
      throw new TypeCheckException("Identifier " + name + " is not a stream (type="
          + symbol.getType());
    }

    StreamSymbol streamSym = (StreamSymbol) symbol;

    // Add a new symbol table layer containing the named stream's symbols.
    SymbolTable sourceTable = new HashSymbolTable(symtab);

    for (TypedField field : streamSym.getFields()) {
      String fieldName = field.getName();
      sourceTable.addSymbol(new Symbol(fieldName, field.getType()));
    }

    // Push it on top of the stack.
    mSymTableContext.push(sourceTable);
  }

  @Override
  protected void visit(SelectStmt s) throws VisitException {
    SQLStatement source = s.getSource();
    if (source instanceof SelectStmt || source instanceof LiteralSource) {
      // This pushes a symbol table on the stack, declaring the fields of this source.
      // Make sure we pop it on our way out.
      source.accept(this);
    } else {
      throw new TypeCheckException("Invalid source in FROM clause; this must be "
          + "an identifier, or a SELECT statement. Got " + source.getClass().getName());
    }

    try {
      SymbolTable fieldsSymTab = mSymTableContext.top();
      FieldList fields = s.getFields();
      if (!fields.isAllFields()) {
        for (String fieldName : fields.getFieldNames()) {
          // Check that this field is defined by one of the input sources.
          // Since the source pushed a symbol table on the stack, just check
          // that we have a symbol table, and that this is a primitive value.
          Symbol fieldSym = fieldsSymTab.resolve(fieldName);
          if (null == fieldSym) {
            throw new TypeCheckException("No field \"" + fieldName + "\" in source");
          }

          Type fieldType = fieldSym.getType();
          if (!fieldType.isPrimitive()) {
            // This name refers to a stream or other complex object. We can't
            // select that.
            throw new TypeCheckException("Cannot select non-primitive entity \""
                + fieldName + "\"");
          }
        }
      }

      // TODO(aaron): Check the where clause for validity if it's nonnull.
    } finally {
      // Pop the source's symbol table from the stack.
      mSymTableContext.pop();
    }
  }

  @Override
  protected void visit(DescribeStmt s) throws VisitException {
    // Check the symbol table that the identifier exists.
    String id = s.getIdentifier();
    SymbolTable symtab = mSymTableContext.top();

    Symbol symbol = symtab.resolve(id);
    if (null == symbol) {
      throw new TypeCheckException("No such identifier: " + id);
    }
  }

  @Override
  protected void visit(ExplainStmt s) throws VisitException {
    s.getChildStmt().accept(this);
  }
}
