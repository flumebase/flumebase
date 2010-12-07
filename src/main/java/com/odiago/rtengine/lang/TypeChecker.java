// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.odiago.rtengine.exec.HashSymbolTable;
import com.odiago.rtengine.exec.StreamSymbol;
import com.odiago.rtengine.exec.Symbol;
import com.odiago.rtengine.exec.SymbolTable;

import com.odiago.rtengine.lang.TypeChecker;

import com.odiago.rtengine.parser.AliasedExpr;
import com.odiago.rtengine.parser.AllFieldsExpr;
import com.odiago.rtengine.parser.BinExpr;
import com.odiago.rtengine.parser.ConstExpr;
import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.DescribeStmt;
import com.odiago.rtengine.parser.DropStmt;
import com.odiago.rtengine.parser.EntityTarget;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.Expr;
import com.odiago.rtengine.parser.FnCallExpr;
import com.odiago.rtengine.parser.IdentifierExpr;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;
import com.odiago.rtengine.parser.ShowStmt;
import com.odiago.rtengine.parser.TypedField;
import com.odiago.rtengine.parser.UnaryExpr;

import com.odiago.rtengine.util.Stack;

/**
 * Run a type-checker over all elements of the AST.
 */
public class TypeChecker extends Visitor {
  private static final Logger LOG = LoggerFactory.getLogger(
      TypeChecker.class.getName());

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
      String fieldName = field.getProjectedName();
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
      // Type check all the selected expressions.
      for (AliasedExpr aliasedExpr : s.getSelectExprs()) {
        aliasedExpr.accept(this);
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

  @Override
  protected void visit(AliasedExpr e) throws VisitException {
    Expr subExpr = e.getExpr();

    // If the sub-expression is just a '*', this can't have an alias.
    // ("SELECT * AS bla FROM ..." is illegal.)
    if (subExpr instanceof AllFieldsExpr && e.getProjectedLabel() != null) {
      throw new TypeCheckException("Cannot assign field label to '*' operator.");
    }

    // Typecheck the sub-expression.
    subExpr.accept(this);
  }

  protected void visit(ConstExpr e) throws VisitException {
    // Nothing to do.
  }

  protected void visit(BinExpr e) throws VisitException {
    // Type-check sub-expressions.
    e.getLeftExpr().accept(this);
    e.getRightExpr().accept(this);

    SymbolTable symTab = mSymTableContext.top();

    // Get the type from the expression; this handles promotion of
    // lhs to rhs or vice versa.
    Type expType = e.getType(symTab);
    if (null == expType) {
      // Sub-expressions cannot agree on a common type.
      throw new TypeCheckException("Cannot assign type to binary expression: "
          + e.toStringOneLine());
    }

    e.setType(expType); // Cache this value for later.

    Type lhsType = e.getLeftExpr().getType(symTab);
    Type rhsType = e.getRightExpr().getType(symTab);

    // Given the operator in the binary expression, check that the type makes sense.
    switch (e.getOp()) {
    case Times:
    case Div:
    case Mod:
    case Subtract:
      // For numeric operators, input types must be numeric.
      if (!lhsType.isNumeric()) {
        throw new TypeCheckException("Operator " + e.getOp() + " requires numeric lhs argument");
      } else if (!rhsType.isNumeric()) {
        throw new TypeCheckException("Operator " + e.getOp() + " requires numeric rhs argument");
      }
      break;
    case Add:
      // This requires input arguments that are numeric, or strings.
      // Check that the output type (which is the resolved, promoted type) is numeric or string.
      if (!expType.isNumeric() && !expType.equals(Type.getPrimitive(Type.TypeName.STRING))) {
        throw new TypeCheckException("Operator " + e.getOp()
            + " requires numeric or string arguments.");
      }
      break;
    case Greater:
    case GreaterEq:
    case Less:
    case LessEq:
      // These require comparable arguments.
      if (!lhsType.isComparable()) {
        throw new TypeCheckException("Operator " + e.getOp()
            + " requires comparable lhs argument.");
      } else if (!rhsType.isComparable()) {
        throw new TypeCheckException("Operator " + e.getOp()
            + " requires comparable rhs argument.");
      }
      break;
    case Eq:
    case NotEq:
      // These require primitive arguments.
      if (!lhsType.isPrimitive()) {
        throw new TypeCheckException("Cannot test for equality on non-primitive lhs");
      } else if (!rhsType.isPrimitive()) {
        throw new TypeCheckException("Cannot test for equality on non-primitive rhs");
      }
      break;
    case IsNot:
    case Is:
      // TODO(aaron): These are unary operators; treat them as such.
      LOG.warn("IsNot/Is binops are not supported by the type checker");
      break;
    case And:
    case Or:
      // Both arguments must be boolean.
      if (!lhsType.equals(Type.getPrimitive(Type.TypeName.BOOLEAN))) {
        throw new TypeCheckException("Operator " + e.getOp() + " requires boolean lhs.");
      } else if (!rhsType.equals(Type.getPrimitive(Type.TypeName.BOOLEAN))) {
        throw new TypeCheckException("Operator " + e.getOp() + " requires boolean rhs.");
      }
      break;
    default:
      throw new TypeCheckException("Do not know how to type-check boolean operator: "
          + e.getOp());
    }
  }

  protected void visit(FnCallExpr e) throws VisitException {
    // TODO(aaron): Write this.
    LOG.warn("Type-checking for function calls not yet implemented");
  }

  protected void visit(IdentifierExpr e) throws VisitException {
    SymbolTable fieldsSymTab = mSymTableContext.top();

    String fieldName = e.getIdentifier();
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

    // Let the AST node memoize its typing information from the symbol table;
    // it will need to reference this at run time to look up values from the
    // EventWrapper.
    e.setType(fieldType);
  }

  protected void visit(UnaryExpr e) throws VisitException {
    // Check that this has a type, that its sub-expression has a type,
    // and that the subexpr type is appropriate for the operator.

    // Start by type-checking the subexpression.
    e.getSubExpr().accept(this);

    SymbolTable symTab = mSymTableContext.top();
    Type expType = e.getType(symTab);
    if (null == expType) {
      throw new TypeCheckException("Cannot resolve type for expression: " + e.toStringOneLine());
    }

    Type subType = e.getSubExpr().getType(symTab);
    switch (e.getOp()) {
    case Plus:
    case Minus:
      if (!subType.isNumeric()) {
        throw new TypeCheckException("Unary " + e.getOp()
            + " operator requires numeric argument");
      }
      break;
    case Not:
      if (!subType.equals(Type.getPrimitive(Type.TypeName.BOOLEAN))) {
        throw new TypeCheckException("Unary " + e.getOp()
            + " operator requires boolean argument");
      }
      break;
    default:
      throw new TypeCheckException("Cannot type-check unary operator " + e.getOp());
    }
  }

  protected void visit(AllFieldsExpr e) throws VisitException {
    // Nothing to do.
  }
}
