// (c) Copyright 2010 Odiago, Inc.

package com.odiago.rtengine.lang;

import com.odiago.rtengine.lang.TypeChecker;

import com.odiago.rtengine.parser.CreateStreamStmt;
import com.odiago.rtengine.parser.ExplainStmt;
import com.odiago.rtengine.parser.FieldList;
import com.odiago.rtengine.parser.LiteralSource;
import com.odiago.rtengine.parser.SQLStatement;
import com.odiago.rtengine.parser.SelectStmt;

/**
 * Run a type-checker over all elements of the AST.
 */
public class TypeChecker extends Visitor {
  @Override
  protected void visit(CreateStreamStmt s) throws VisitException {
    // Nothing to do.
  }

  @Override
  protected void visit(LiteralSource s) throws VisitException {
    // TODO(aaron): Check the symbol table that we have a stream with the name
    // in this LiteralSource.
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
  }

  @Override
  protected void visit(ExplainStmt s) throws VisitException {
    s.getChildStmt().accept(this);
  }
}
