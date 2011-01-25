// (c) Copyright 2010 Odiago, Inc.

lexer grammar SqlLexer;

options {
  language = Java;
  superClass = AbstractSqlLexer;
}

@header {
  package com.odiago.rtengine.parser;
}

L_AND : A N D ;
AS : A S ;
BETWEEN : B E T W E E N ;
BIGINT : B I G I N T ;
BOOLEAN : B O O L E A N ;
CREATE : C R E A T E ;
DAYS : D A Y S ;
DESCRIBE : D E S C R I B E ;
DOUBLE : D O U B L E ;
DROP : D R O P ;
EVENT : E V E N T ;
EXPLAIN : E X P L A I N ;
FALSE : F A L S E ;
FILE : F I L E ;
FLOAT : F L O A T ;
FLOW : F L O W ;
FLOWS : F L O W S ;
FOLLOWING: F O L L O W I N G ;
FORMAT : F O R M A T ;
FROM : F R O M ;
FUNCTIONS : F U N C T I O N S ;
HOURS : H O U R S ;
INTERVAL : I N T E R V A L ;
INT_KW : I N T ;
IS : I S ;
JOIN : J O I N ;
LOCAL : L O C A L ;
MINUTES : M I N U T E S ;
MONTHS : M O N T H S ;
NODE : N O D E ;
NOT : N O T ;
NULL : N U L L ;
ON : O N ;
L_OR : O R ;
OVER : O V E R ;
PRECEDING: P R E C E D I N G ;
PROPERTIES : P R O P E R T I E S ;
RANGE : R A N G E ;
SECONDS : S E C O N D S ;
SELECT : S E L E C T ;
SHOW : S H O W ;
SOURCE : S O U R C E ;
STREAM : S T R E A M ;
STREAMS : S T R E A M S ;
STRING_KW : S T R I N G ;
TIMESTAMP : T I M E S T A M P ;
TRUE : T R U E ;
WEEKS : W E E K S ;
WINDOW : W I N D O W ;
WHERE : W H E R E ;
YEARS : Y E A R S ;

ID  : ('a'..'z'|'A'..'Z'|'_') ('a'..'z'|'A'..'Z'|'0'..'9'|'_')*
    ;

DOT : '.';

STAR : '*';

LPAREN : '(';

RPAREN : ')';

COMMA : ',';

PLUS : '+';

MINUS : '-';

SLASH : '/';

PERCENT : '%';

GTEQ : '>' '=';

GT : '>';

LTEQ : '<' '=';

LT : '<';

EQ : '=';

NEQ : '!' '=';

INT : '0'..'9'+
    ;


COMMENT
    :   '//' ~('\n'|'\r')* '\r'? '\n' {skip();}
    |   '/*' ( options {greedy=false;} : . )* '*/' {skip();}
    ;

WS  :   ( ' '
        | '\t'
        | '\r'
        | '\n'
        ) {skip();}
    ;

QQ_STRING
    :  '"' ( ESC_SEQ | ~('\\'|'"') )* '"'
    ;

Q_STRING
    : '\'' ( ESC_SEQ | ~('\\'|'\'') )* '\''
    ;


fragment
HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;

fragment
ESC_SEQ
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESC
    |   OCTAL_ESC
    ;

fragment
OCTAL_ESC
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

fragment
UNICODE_ESC
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;



// Fragments for individual letters for case-insensitivity in keywords.

fragment A : 'a' | 'A';
fragment B : 'b' | 'B';
fragment C : 'c' | 'C';
fragment D : 'd' | 'D';
fragment E : 'e' | 'E';
fragment F : 'f' | 'F';
fragment G : 'g' | 'G';
fragment H : 'h' | 'H';
fragment I : 'i' | 'I';
fragment J : 'j' | 'J';
fragment K : 'k' | 'K';
fragment L : 'l' | 'L';
fragment M : 'm' | 'M';
fragment N : 'n' | 'N';
fragment O : 'o' | 'O';
fragment P : 'p' | 'P';
fragment Q : 'q' | 'Q';
fragment R : 'r' | 'R';
fragment S : 's' | 'S';
fragment T : 't' | 'T';
fragment U : 'u' | 'U';
fragment V : 'v' | 'V';
fragment W : 'w' | 'W';
fragment X : 'x' | 'X';
fragment Y : 'y' | 'Y';
fragment Z : 'z' | 'Z';

