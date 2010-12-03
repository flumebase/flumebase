// (c) Copyright 2010 Odiago, Inc.
//
// Main parse tree for the RTSQL grammar.

parser grammar SqlGrammar;

options {
  language = Java;
  output = AST;
  superClass = AbstractSqlParse;
  tokenVocab = SqlLexer;
}

@header {
  package com.odiago.rtengine.parser;

  import com.odiago.rtengine.lang.Type;
}

top returns [SQLStatement val]:
  s=stmt {$val = $s.val;} EOF;

stmt returns [SQLStatement val]:
    cs=stmt_create_stream {$val = $cs.val;}
  | sel=stmt_select {$val = $sel.val;}
  | expl=stmt_explain {$val = $expl.val;}
  | desc=stmt_describe {$val = $desc.val;}
  | show=stmt_show {$val = $show.val;}
  | drop=stmt_drop {$val = $drop.val;}
  ;

stmt_create_stream returns [CreateStreamStmt val]:
    CREATE STREAM fid=stream_sel ft=typed_field_list FROM LOCAL FILE f=user_sel
        {
          $val = new CreateStreamStmt($fid.val,
              StreamSourceType.File, $f.val, true, $ft.val);
        }
  | CREATE STREAM sid=stream_sel st=typed_field_list FROM slcl=LOCAL? SOURCE src=user_sel
        {
          boolean srcIsLocal = slcl != null;
          $val = new CreateStreamStmt($sid.val,
              StreamSourceType.Sink, $src.val, srcIsLocal, $st.val);
        }
  ;

stmt_describe returns [DescribeStmt val]:
  DESCRIBE id=user_sel {$val = new DescribeStmt($id.val);};

stmt_explain returns [ExplainStmt val]:
  EXPLAIN s=stmt {$val = new ExplainStmt($s.val);};

stmt_select returns [SelectStmt val]:
  SELECT e=aliased_expr_list FROM s=source_definition w=optional_where_conditions
  {$val = new SelectStmt($e.val, $s.val, $w.val);};

stmt_show returns [ShowStmt val]:
    SHOW FLOWS {$val = new ShowStmt(EntityTarget.Flow);}
  | SHOW STREAMS {$val = new ShowStmt(EntityTarget.Stream);};

stmt_drop returns [DropStmt val]:
    DROP FLOW f=user_sel {$val = new DropStmt(EntityTarget.Flow, $f.val);}
  | DROP STREAM s=stream_sel {$val = new DropStmt(EntityTarget.Stream, $s.val);};

// Expressions involve operators of varying precedence.
// Operator precedence is the same as in Java.
// All operators evaluate left-to-right.
//
// Highest priority
//   unary operators:           + - NOT
//   multiplicative:            * / %
//   additive:                  + -
//   comparison:                > < <= >=
//   equality:                  = != IS  IS NOT
//   logical AND:               AND
//   logical OR:                OR
//   function call:             f(e1, e2, e3...)
//   identifiers and constants: x  42 'hello!'
// Lowest priority


// Set of expressions that may have a name applied to them ("SELECT 1+1 AS two ...").
aliased_expr_list returns [List<AliasedExpr> val]:
    ( e1=aliased_expr { $val = new ArrayList<AliasedExpr>(); $val.add($e1.val); }
      ( COMMA e2=aliased_expr { $val.add($e2.val); } )* )?;

aliased_expr returns [AliasedExpr val]:
    e=expr { $val = new AliasedExpr($e.val); }
        ( AS? u=user_sel { ((AliasedExpr) $val).setUserLabel($u.val); } )?
  ;

expr returns [Expr val]: e=or_expr { $val=$e.val; };

or_expr returns [Expr val]:
    a1=and_expr { $val=$a1.val; }
        ( L_OR a2=and_expr { $val = new BinExpr($val, BinOp.Or, $a2.val); } )*
  ;

and_expr returns [Expr val]:
    a1=eq_expr { $val=$a1.val; }
        ( L_AND a2=eq_expr { $val = new BinExpr($val, BinOp.And, $a2.val); } )*
  ;

// TODO: Figure out how to add "IS NOT" without violating LL(*)
eq_expr returns [Expr val]:
    a1=comp_expr { $val=$a1.val; }
        ( op=eq_op a2=comp_expr { $val = new BinExpr($val, $op.val, $a2.val); } )*
  ;

eq_op returns [BinOp val]:
    EQ { $val = BinOp.Eq; }
  | NEQ { $val = BinOp.NotEq; }
  | IS { $val = BinOp.Is; }
  ;

comp_expr returns [Expr val]:
    a1=add_expr { $val=$a1.val; }
        ( op=comp_op a2=add_expr { $val = new BinExpr($val, $op.val, $a2.val); } )*
  ;

comp_op returns [BinOp val]:
    GT { $val = BinOp.Greater; }
  | GTEQ { $val = BinOp.GreaterEq; }
  | LT { $val = BinOp.Less; }
  | LTEQ { $val = BinOp.LessEq; }
  ;

add_expr returns [Expr val]:
    a1=mul_expr { $val=$a1.val; }
        ( op=add_op a2=mul_expr { $val = new BinExpr($val, $op.val, $a2.val); } )*
  ;

add_op returns [BinOp val]:
    PLUS { $val = BinOp.Add; }
  | MINUS { $val = BinOp.Subtract; }
  ;

mul_expr returns [Expr val]:
    a1=unary_expr { $val=$a1.val; }
        ( op=mul_op a2=unary_expr { $val = new BinExpr($val, $op.val, $a2.val); } )*
  ;

mul_op returns [BinOp val]:
    STAR { $val = BinOp.Times; }
  | SLASH { $val = BinOp.Div; }
  | PERCENT { $val = BinOp.Mod; }
  ;

unary_expr returns [Expr val]:
    op=un_op pe=atom_expr { $val = new UnaryExpr($op.val, $pe.val); }
  | a=atom_expr { $val = $a.val; }
  ;

un_op returns [UnaryOp val]:
    PLUS { $val = UnaryOp.Plus; }
  | MINUS { $val = UnaryOp.Minus; }
  | NOT { $val = UnaryOp.Not; }
  ;

//  |  fn=user_sel LPAREN { $val = new FnCallExpr($fn.val); }
//      ( e1=expr { $val.addArg($e1.val); } 
//        ( COMMA e2=expr { $val.addArg($e2.val); } )* )?
//      RPAREN
//  ;

// TODO: numbers with a decimal place. BIGINT-valued integers.
atom_expr returns [Expr val]:
    LPAREN e=expr RPAREN { $val=$e.val; }
  | u=user_sel { $val = new IdentifierExpr($u.val); } // It seems like just an identifier..
    ( LPAREN { $val = new FnCallExpr($u.val); } // (Actually, it's a function call).
      ( e1=expr { ((FnCallExpr) $val).addArg($e1.val); } 
        ( COMMA e2=expr { ((FnCallExpr) $val).addArg($e2.val); } )* )?
      RPAREN
    ) ?
  | TRUE { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE); }
  | FALSE { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE); }
  | i=INT { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf($i.text)); }
  | NULL { $val = new ConstExpr(Type.getNullable(Type.TypeName.ANY), null); }
  | s=Q_STRING { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.STRING), unescape($s.text)); }
  | STAR { $val = new AllFieldsExpr(); }
  ;

// This is a selector for fields; it can be '*' or 'foo, bar, "baz and quux", biff, buff...'
// NOTE: This is not used in SELECT; it's hanging out here waiting to be applied in GROUP BY, etc.
field_list returns [FieldList val]:
    STAR { $val = new AllFieldsList(); }
  | e=explicit_field_list { $val = $e.val; };

explicit_field_list returns [FieldList val]:
  f=field_sel { $val = new FieldList($f.val); } 
  (COMMA g = field_sel {$val.addField($g.val);})*;

// Selecting individual fields is done via user-specified symbol selectors.
field_sel returns [String val] : s=user_sel {$val=$s.val;};

// Specifying a list of fields is done with comma-separated field specs inside parens.
// e.g., '(foo INT, bar STRING , baz STRING ...)'
// At least one field must be specified in this list.
typed_field_list returns [TypedFieldList val]:
  LPAREN tf=field_spec { $val = new TypedFieldList($tf.val); }
  (COMMA tg = field_spec {$val.addField($tg.val);})* RPAREN;

// Specifying a new field is done by giving a selector and a type.
field_spec returns [TypedField val] :
  f=field_sel t=field_type { $val = new TypedField($f.val, $t.val); };

// Types users can apply to a field can be a primitive type with or without a "NOT NULL".
field_type returns [Type val] :
    prim=primitive_field_type nonnul=non_nul_qualifier
    {
      if (nonnul.val) {
        $val = Type.getPrimitive(Type.TypeName.valueOf($prim.val));
      } else {
        $val = Type.getNullable(Type.TypeName.valueOf($prim.val));
      } 
    };

// A non-recursive field type.
primitive_field_type returns [String val]:
    BOOLEAN { $val = "BOOLEAN"; }
  | BIGINT { $val = "BIGINT"; }
  | INT_KW { $val = "INT"; }
  | FLOAT { $val = "FLOAT"; }
  | DOUBLE { $val = "DOUBLE"; }
  | STRING_KW { $val = "STRING"; }
  | TIMESTAMP { $val = "TIMESTAMP"; };


// boolean flag indicating whether "..NOT NULL" was appended to a type spec.
non_nul_qualifier returns [boolean val] :
    NOT NULL { $val = true; }
  | { $val = false; };
    
// A named stream selector is a user-specified symbol selector.
stream_sel returns [String val] : s=user_sel {$val=$s.val;};

// User-selected symbols can be specified as an identifier or a "double-quoted string".
user_sel returns [String val] :
    ID {$val=$ID.text.toLowerCase();}
  | QQ_STRING {$val=unescape($QQ_STRING.text);};

// Source for a SELECT statement (in the FROM clause). For now, must be a
// named stream.
source_definition returns [SQLStatement val]:
    s=stream_sel { $val = new LiteralSource($s.val); }
  | LPAREN st=stmt_select RPAREN { $val = $st.val; };

// WHERE conditions for a SELECT statement. May be omitted.
// Currently takes a string as a regex. TODO(aaron): Make this a proper bexp.
optional_where_conditions returns [WhereConditions val] :
    {$val=null;}
  | WHERE s=Q_STRING {$val=new WhereConditions(unescape($s.text));};

