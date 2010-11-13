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

top : stmt EOF;

stmt :
    stmt_create_stream
  | stmt_select
  ;

stmt_create_stream : CREATE STREAM ID;

stmt_select : SELECT field_list FROM source_definition
    optional_conditions;

// This can be '*' or 'foo, bar, "baz and quux", biff, buff...'
field_list : ALL_FIELDS | explicit_field_list;

explicit_field_list : field_spec 
  | field_spec COMMA explicit_field_list;

// Individual fields can be specified as an identifier or a "quoted string".
field_spec : ID | STRING;


// Source for a SELECT statement (in the FROM clause). For now, must be a
// named stream.
source_definition : ID | STRING | LPAREN stmt_select RPAREN;

// WHERE conditions for a SELECT statement. May be omitted.
// Currently takes a string as a regex. TODO(aaron): Make this a proper bexp.
optional_conditions :
  | WHERE STRING;

