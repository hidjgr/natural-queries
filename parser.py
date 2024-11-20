import ply.lex as lex
import ply.yacc as yacc

# Lexer setup with tokens and reserved words
tokens = [
    'IDENTIFIER', 'NUMBER', 'STRING',
    'EQUALS', 'COMMA', 'DOT', 'LPAREN', 'RPAREN', 'LBRACKET', 'RBRACKET'
]

# df.filter(exp=[...]).group(by="", agg="").op(df2.filter())
# DF.METHOD(KWARG).METHOD(KWARGS)

# Reserved keywords (method names)
#reserved = {
#    'group': 'GROUP',
#    'filter': 'FILTER',
#    'join': 'JOIN',
#    'op' : 'OP'
#}
#tokens += list(reserved.values())

# Token definitions
t_EQUALS = r'='
t_COMMA = r','
t_DOT = r'\.'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'

def t_IDENTIFIER(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    #t.type = reserved.get(t.value, 'IDENTIFIER')
    return t

def t_STRING(t):
    r'\".*?\"|\'.*?\''
    t.value = t.value[1:-1]  # Strip quotes
    return t

def t_NUMBER(t):
    r'\d+'
    t.value = int(t.value)
    return t

# Ignored characters
t_ignore = ' \t'

# Error handling for illegal characters
def t_error(t):
    print(f"Illegal character '{t.value[0]}' at line {t.lineno}, position {t.lexpos}")
    t.lexer.skip(1)

# Build the lexer
lexer = lex.lex()

# Parser setup with detailed error handling
# Rules:
# exp
#  → iden
#  | iden calls
#          → DOT call
#          | DOT call calls
#                 → GROUP RPAREN group_kwargs LPAREN
#                 | FILTER RPAREN filter_kwargs LPAREN
#                 | JOIN RPAREN join_kwargs LPAREN
#                 | OP RPAREN op_kwargs LPAREN

# Define parsing rules
def p_expression(p):
    '''expression : IDENTIFIER transformations
                  | IDENTIFIER'''

def p_transformations(p):
    '''transformations : DOT IDENTIFIER LPAREN kwargs RPAREN transformations
                       | DOT IDENTIFIER LPAREN kwargs RPAREN'''

def p_kwargs(p):
    '''kwargs : kwarg COMMA kwargs
              | kwarg'''

def p_kwarg(p):
    '''kwarg : IDENTIFIER EQUALS NUMBER
             | IDENTIFIER EQUALS STRING'''

# Build the parser
parser = yacc.yacc()
