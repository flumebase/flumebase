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
}

top returns [SQLStatement val]:
  s=stmt {$val = $s.val;} EOF;

stmt returns [SQLStatement val]:
    cs=stmt_create_stream {$val = $cs.val;}
  | sel=stmt_select {$val = $sel.val;}
  | expl=stmt_explain {$val = $expl.val;}
  | desc=stmt_describe {$val = $desc.val;}
  ;

stmt_create_stream returns [CreateStreamStmt val]:
    CREATE STREAM fid=stream_spec FROM LOCAL FILE f=user_spec
        {$val = new CreateStreamStmt($fid.val, StreamSourceType.File, $f.val, true);}
  | CREATE STREAM sid=stream_spec FROM slcl=LOCAL? SOURCE src=user_spec
        {
          boolean srcIsLocal = slcl != null;
          $val = new CreateStreamStmt($sid.val, StreamSourceType.Sink, $src.val, srcIsLocal);
        }
  ;

stmt_describe returns [DescribeStmt val]:
  DESCRIBE id=user_spec {$val = new DescribeStmt($id.val);};

stmt_explain returns [ExplainStmt val]:
  EXPLAIN s=stmt {$val = new ExplainStmt($s.val);};

stmt_select returns [SelectStmt val]:
  SELECT f=field_list FROM s=source_definition w=optional_where_conditions
  {$val = new SelectStmt($f.val, $s.val, $w.val);};

// This can be '*' or 'foo, bar, "baz and quux", biff, buff...'
field_list returns [FieldList val]:
    ALL_FIELDS { $val = new AllFieldsList(); }
  | e=explicit_field_list { $val = $e.val; };

explicit_field_list returns [FieldList val]:
  f=field_spec { $val = new FieldList($f.val); } 
  (COMMA g = field_spec {$val.addField($g.val);})*;

// Individual fields are user-specified symbols.
field_spec returns [String val] : s=user_spec {$val=$s.val;};

// A named stream is a user-specified symbol.
stream_spec returns [String val] : s=user_spec {$val=$s.val;};

// User-specified symbols can be specified as an identifier or a "quoted string".
user_spec returns [String val] :
    ID {$val=$ID.text.toLowerCase();}
  | STRING {$val=unescape($STRING.text);};

// Source for a SELECT statement (in the FROM clause). For now, must be a
// named stream.
source_definition returns [SQLStatement val]:
    s=user_spec { $val = new LiteralSource($s.val); }
  | LPAREN st=stmt_select RPAREN { $val = $st.val; };

// WHERE conditions for a SELECT statement. May be omitted.
// Currently takes a string as a regex. TODO(aaron): Make this a proper bexp.
optional_where_conditions returns [WhereConditions val] :
    {$val=null;}
  | WHERE s=STRING {$val=new WhereConditions(unescape($s.text));};

