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
  SELECT f=field_list FROM s=source_definition w=optional_where_conditions
  {$val = new SelectStmt($f.val, $s.val, $w.val);};

stmt_show returns [ShowStmt val]:
    SHOW FLOWS {$val = new ShowStmt(EntityTarget.Flow);}
  | SHOW STREAMS {$val = new ShowStmt(EntityTarget.Stream);};

stmt_drop returns [DropStmt val]:
    DROP FLOW f=user_sel {$val = new DropStmt(EntityTarget.Flow, $f.val);}
  | DROP STREAM s=stream_sel {$val = new DropStmt(EntityTarget.Stream, $s.val);};

// This is a selector for fields; it can be '*' or 'foo, bar, "baz and quux", biff, buff...'
field_list returns [FieldList val]:
    ALL_FIELDS { $val = new AllFieldsList(); }
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

// User-selected symbols can be specified as an identifier or a "quoted string".
user_sel returns [String val] :
    ID {$val=$ID.text.toLowerCase();}
  | STRING {$val=unescape($STRING.text);};

// Source for a SELECT statement (in the FROM clause). For now, must be a
// named stream.
source_definition returns [SQLStatement val]:
    s=stream_sel { $val = new LiteralSource($s.val); }
  | LPAREN st=stmt_select RPAREN { $val = $st.val; };

// WHERE conditions for a SELECT statement. May be omitted.
// Currently takes a string as a regex. TODO(aaron): Make this a proper bexp.
optional_where_conditions returns [WhereConditions val] :
    {$val=null;}
  | WHERE s=STRING {$val=new WhereConditions(unescape($s.text));};

