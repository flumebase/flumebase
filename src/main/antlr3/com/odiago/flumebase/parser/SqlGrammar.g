//
// Licensed to Odiago, Inc. under one or more contributor license
// agreements.  See the NOTICE.txt file distributed with this work for
// additional information regarding copyright ownership.  Odiago, Inc.
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.
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
  package com.odiago.flumebase.parser;

  import com.odiago.flumebase.lang.ListType;
  import com.odiago.flumebase.lang.NullableType;
  import com.odiago.flumebase.lang.PreciseType;
  import com.odiago.flumebase.lang.Type;

  import org.apache.avro.util.Utf8;
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

stmt_create_stream returns [SQLStatement val]:
    CREATE STREAM sid=stream_sel fields=typed_field_list FROM lcl=LOCAL? st=src_type src=src_spec
        {
          boolean srcIsLocal = lcl != null;
          $val = new CreateStreamStmt($sid.val,
              $st.val, $src.val, srcIsLocal, $fields.val);
        }
      sfmt=optional_format_spec { ((CreateStreamStmt) $val).setFormatSpec($sfmt.val); }
  | CREATE STREAM nm=stream_sel AS sel=stmt_select
        {
          $sel.val.setOutputName($nm.val);
          $val = $sel.val;
        }
  ;

stmt_describe returns [DescribeStmt val]:
  DESCRIBE id=user_sel {$val = new DescribeStmt($id.val);};

stmt_explain returns [ExplainStmt val]:
  EXPLAIN s=stmt {$val = new ExplainStmt($s.val);};

stmt_select returns [SelectStmt val]:
  SELECT e=aliased_expr_list FROM s=source_definition w=optional_where_conditions
  g=optional_group_by over=optional_window_over
  h=optional_having
  wins=optional_window_defs
  { $val = new SelectStmt($e.val, $s.val, $w.val, $g.val, $over.val, $h.val, $wins.val); };

stmt_show returns [ShowStmt val]:
    SHOW FLOWS {$val = new ShowStmt(EntityTarget.Flow);}
  | SHOW STREAMS {$val = new ShowStmt(EntityTarget.Stream);}
  | SHOW FUNCTIONS {$val = new ShowStmt(EntityTarget.Function);}
  ;

stmt_drop returns [DropStmt val]:
    DROP FLOW f=user_sel {$val = new DropStmt(EntityTarget.Flow, $f.val);}
  | DROP STREAM s=stream_sel {$val = new DropStmt(EntityTarget.Stream, $s.val);};

// Expressions involve operators of varying precedence.
// Operator precedence is the same as in Java.
// All operators evaluate left-to-right.
//
// Highest priority
//   unary null operators:      IS NULL, IS NOT NULL
//   unary operators:           + - NOT
//   multiplicative:            * / %
//   additive:                  + -
//   comparison:                > < <= >=
//   equality:                  = !=
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
        ( AS? u=user_sel { ((AliasedExpr) $val).setUserAlias($u.val); } )?
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

eq_expr returns [Expr val]:
    a1=comp_expr { $val=$a1.val; }
        ( op=eq_op a2=comp_expr { $val = new BinExpr($val, $op.val, $a2.val); } )*
  ;

eq_op returns [BinOp val]:
    EQ { $val = BinOp.Eq; }
  | NEQ { $val = BinOp.NotEq; }
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
    op=un_op pe=unary_null_expr { $val = new UnaryExpr($op.val, $pe.val); }
  | a=unary_null_expr { $val = $a.val; }
  ;

un_op returns [UnaryOp val]:
    PLUS { $val = UnaryOp.Plus; }
  | MINUS { $val = UnaryOp.Minus; }
  | NOT { $val = UnaryOp.Not; }
  ;

unary_null_expr returns [Expr val]:
    e=atom_expr { $val = $e.val; }
    (IS { $val = new UnaryExpr(UnaryOp.IsNull, $e.val); }
     (n=NOT { if ($n != null) { ((UnaryExpr)$val).setOp(UnaryOp.IsNotNull); } } )?
     NULL)?;

// TODO: numbers with a decimal place. BIGINT-valued integers.
atom_expr returns [Expr val]:
    LPAREN e=expr RPAREN { $val=$e.val; }
  | u=maybe_qualified_user_sel { $val = new IdentifierExpr($u.val); } // An identifier.
    ( LPAREN { $val = new FnCallExpr($u.val); } // (Actually, it's a function call).
      ( e1=expr { ((FnCallExpr) $val).addArg($e1.val); } 
        ( COMMA e2=expr { ((FnCallExpr) $val).addArg($e2.val); } )* )?
      RPAREN
    ) ?
  | TRUE { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.TRUE); }
  | FALSE { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.BOOLEAN), Boolean.FALSE); }
  | i=INT { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.INT), Integer.valueOf($i.text)); }
  | NULL { $val = new ConstExpr(Type.getNullable(Type.TypeName.NULL), null); }
  | s=Q_STRING { $val = new ConstExpr(Type.getPrimitive(Type.TypeName.STRING),
      new Utf8(unescape($s.text))); }
  | STAR { $val = new AllFieldsExpr(); }
  ;

// Selecting individual fields is done via user-specified symbol selectors.
field_sel returns [String val] : s=maybe_qualified_user_sel {$val=$s.val;};

// For specifying a list of fields (e.g., in a GROUP BY x, y, z...)
field_sel_list returns [List<String> val] :
    { $val = new ArrayList<String>(); }
    ( f=field_sel { $val.add($f.val); }
      ( COMMA f2=field_sel { $val.add($f2.val); } )*
    )?;

// Specifying a list of fields is done with comma-separated field specs inside parens.
// e.g., '(foo INT, bar STRING , baz STRING ...)'
// At least one field must be specified in this list.
typed_field_list returns [TypedFieldList val]:
  LPAREN tf=field_spec { $val = new TypedFieldList($tf.val); }
  (COMMA tg=field_spec {$val.addField($tg.val);})* RPAREN;

// Specifying a new field is done by giving a selector and a type.
field_spec returns [TypedField val] :
  f=field_sel t=field_type { $val = new TypedField($f.val, $t.val); };

// Types users can apply to a field can be a scalar type, or a list type (either with
// or without 'NOT NULL').
field_type returns [Type val] :
    s=scalar_field_type { $val = $s.val; }
  | lst=list_type { $val = $lst.val; }
  ;

// Any non-recursive field type, either a simple primitive, or a PRECISE type.
scalar_field_type returns [Type val] :
    prim=primitive_field_type nonnul=non_nul_qualifier
    {
      try {
        if (nonnul.val) {
          $val = Type.getPrimitive(Type.TypeName.valueOf($prim.val));
        } else {
          $val = Type.getNullable(Type.TypeName.valueOf($prim.val));
        } 
      } catch (NullPointerException npe) {
        $val = Type.getPrimitive(Type.TypeName.TYPECLASS_ANY);
      }
    }
  | precise=precise_type { $val = $precise.val; }
  ;

// A non-recursive field type.
primitive_field_type returns [String val]:
    BOOLEAN { $val = "BOOLEAN"; }
  | BINARY { $val = "BINARY"; }
  | BIGINT { $val = "BIGINT"; }
  | INT_KW { $val = "INT"; }
  | FLOAT { $val = "FLOAT"; }
  | DOUBLE { $val = "DOUBLE"; }
  | STRING_KW { $val = "STRING"; }
  | TIMESTAMP { $val = "TIMESTAMP"; }
  ;

// Defines a PRECISE(n) type.
precise_type returns [Type val] :
    PRECISE LPAREN precision=integer RPAREN
    { $val = new NullableType(new PreciseType($precision.val)); }
    (NOT NULL { $val = ((NullableType) $val).getInnerType(); })?
  ;

// An arbitrary-length list of values of the same type.
list_type returns [Type val]:
    LIST LT s=scalar_field_type GT { $val = new NullableType(new ListType($s.val)); }
    (NOT NULL { $val = ((NullableType) $val).getInnerType(); })?
  ;

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

// A user-selected symbol which may be an identifier, a qualified.identifier, or a
// "double-quoted string".
maybe_qualified_user_sel returns [String val] :
    id=ID { $val = $id.text.toLowerCase(); }
    ( DOT id2=ID { $val = $val + "." + $id2.text.toLowerCase(); } )?
  | QQ_STRING {$val=unescape($QQ_STRING.text);};
    
// Flume source, filename, etc. is a 'single quoted string.'
src_spec returns [String val] :
    q=Q_STRING { $val=unescape($q.text); };

// Source for a SELECT statement (in the FROM clause). This is a named stream
// or a subquery, optionally joined with one or more sources.
source_definition returns [RecordSource val]:
    s=stream_sel { $val = new LiteralSource($s.val); }
    ( AS? alias=stream_sel { ((LiteralSource) $val).setAlias($alias.val); } )?
    ( JOIN j=source_definition ON e=expr OVER w=inline_window_spec
      { $val = new JoinedSource($val, $j.val, $e.val, $w.val); }
    )*
  | LPAREN st=stmt_select RPAREN { $val = $st.val; }
    ( AS? alias=stream_sel { ((SelectStmt) $val).setAlias($alias.val); } )?
  ;

// Indicates in a CREATE STREAM statement what the type of source is. 
src_type returns [StreamSourceType val]:
    FILE { $val = StreamSourceType.File; }
  | SOURCE { $val = StreamSourceType.Source; }
  | NODE { $val = StreamSourceType.Node; }
  ;
    

// WHERE conditions for a SELECT statement. May be omitted.
// Returns an expression to be evaluated, or null if it is omitted.
optional_where_conditions returns [Expr val] :
    { $val=null; }
  | WHERE e=expr { $val=$e.val; }
  ;


// HAVING conditions for a SELECT statement. May be omitted.
// Returns an expression to be evaluated, or null if it is omitted.
optional_having returns [Expr val] :
    { $val=null; }
  | HAVING e=expr { $val=$e.val; }
  ;

// GROUP BY clause for a SELECT statement. May be omitted.
// Returns a group by condition, or null if it is omitted.
optional_group_by returns [GroupBy val] :
    { $val=null; }
  | GROUP BY f=field_sel_list { $val = new GroupBy($f.val); }
  ;

// OVER clause for a SELECT statement. May be omitted.
// Returns a WindowSpec over which we aggregate, or null if it is omitted.
optional_window_over returns [Expr val]:
    { $val=null; }
  | OVER w=inline_window_spec { $val = $w.val; };

// Set of WINDOW x AS ... definitions for a SELECT statement.
optional_window_defs returns [List<WindowDef> val] :
    { $val = new ArrayList<WindowDef>(); }
    ( WINDOW id=user_sel AS LPAREN w=window_spec RPAREN
      { $val.add(new WindowDef($id.val, $w.val)); }
      ( COMMA WINDOW id2=user_sel AS LPAREN w2=window_spec RPAREN
        { $val.add(new WindowDef($id2.val, $w2.val)); } )* )?;

// Specifies a window within which join and aggregation operators work.
window_spec returns [WindowSpec val]:
    RANGE r=range_spec { $val = new WindowSpec($r.val); };

// Returns a window specifier itself, or an identifier which encompasses a window.
// This defines all the forms a window definition may take on, "inline" in a statement.
inline_window_spec returns [Expr val]:
    w=window_spec { $val = $w.val; }
  | id=user_sel { $val = new IdentifierExpr($id.val); }
  ;

// Defines an interval of time
range_spec returns [RangeSpec val]:
    INTERVAL e=expr t=time_width PRECEDING { $val = new RangeSpec($e.val, $t.val); }
  | BETWEEN INTERVAL e1=expr t1=time_width PRECEDING
    L_AND INTERVAL e2=expr t2=time_width FOLLOWING
    { $val = new RangeSpec($e1.val, $t1.val, $e2.val, $t2.val); }
  ;

time_width returns [TimeWidth val]:
    SECONDS { $val = TimeWidth.Seconds; }
  | MINUTES { $val = TimeWidth.Minutes; }
  | HOURS { $val = TimeWidth.Hours; }
  | DAYS { $val = TimeWidth.Days; }
  | WEEKS { $val = TimeWidth.Weeks; }
  | MONTHS { $val = TimeWidth.Months; }
  | YEARS { $val = TimeWidth.Years; }
  ;

// Specifies how input records of the stream should be parsed.
// e.g.: CREATE STREAM .... EVENT FORMAT 'delimited' PROPERTIES ('delim.char' = '\t');
optional_format_spec returns [FormatSpec val] :
    { $val = new FormatSpec(); }
  | EVENT FORMAT fmt=Q_STRING { $val = new FormatSpec(unescape($fmt.text)); }
    (PROPERTIES LPAREN ( k=Q_STRING EQ v=Q_STRING {
        $val.setParam(unescape($k.text), unescape($v.text)); }
        ( COMMA k2=Q_STRING EQ v2=Q_STRING {
        $val.setParam(unescape($k2.text), unescape($v2.text)); } )*
    )? RPAREN)?
  ;


integer returns [Integer val]:
    i=INT { $val = Integer.valueOf($i.text); };

