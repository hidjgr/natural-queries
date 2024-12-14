import ply.lex as lex
import ply.yacc as yacc
from functools import reduce

## Lexer
# Tokens
tokens = [
    'IDENTIFIER', # dataframe name, function call, kwarg name
    'NUMBER', # kwarg value
    'STRING', # kwarg value
    'DOT', # function calls
    'LPAREN', 'RPAREN', # function calls
    'EQUALS', # kwargs
    'COMMA', # kwarg sep and list sep
    'LBRACKET', 'RBRACKET'
]

# Token definitions
t_EQUALS = r'='
t_COMMA = r','
t_DOT = r'\.'
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_IDENTIFIER = r'[a-zA-Z_][a-zA-Z0-9_]*'

def t_STRING(t):
    r'\".*?\"|\'.*?\''
    t.value = t.value[1:-1]  # Strip quotes
    return t

def t_NUMBER(t):
    r"[-+]?(?:\d*\.*\d+)"
    t.value = int(t.value) if t.value.isnumeric() else float(t.value)
    return t

# Ignored characters
t_ignore = ' \t'

# Error handling for illegal characters
def t_error(t):
    print(f"Illegal character '{t.value[0]}' at line {t.lineno}, position {t.lexpos}")
    t.lexer.skip(1)

# Build the lexer
lexer = lex.lex()


## Parser
def p_expression(p):
    '''expression : IDENTIFIER functions
                  | IDENTIFIER'''

    if len(p) == 3:
        p[0] = (p[1], lambda qlf: reduce(lambda x,y: getattr(x, y[0])(**y[1]), p[2], qlf))
    else:
        p[0] = (p[1], lambda qlf: qlf)

def p_functions(p):
    '''functions : DOT IDENTIFIER LPAREN kwargs RPAREN functions
                 | DOT IDENTIFIER LPAREN kwargs RPAREN'''

    if len(p) == 7:
        p[0] = [(p[2], p[4])] + p[6]
    else:
        p[0] = [(p[2], p[4])]

def p_kwargs(p):
    '''kwargs : IDENTIFIER EQUALS kwarg_arg COMMA kwargs
              | IDENTIFIER EQUALS kwarg_arg'''

    if len(p) == 6:
        p[0] = {p[1]: p[3]} | p[5]
    else:
        p[0] = {p[1]: p[3]}


def p_kwarg(p):
    '''kwarg_arg : STRING
                 | NUMBER
                 | string_list
                 | expression
                 | filter_expression'''

    p[0] = p[1]

def p_string_list(p):
    '''string_list : LBRACKET strings RBRACKET'''

    p[0] = p[2]


def p_strings(p):
    '''strings : STRING COMMA strings
               | STRING'''

    if len(p) == 4:
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]


def p_filter_expressions(p):
    '''filter_expressions : filter_expression COMMA filter_expressions
                          | filter_expression'''

    if len(p) == 4:
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]


def p_filter_expression(p):
    '''filter_expression : filter_node
                         | filter_leaf'''

    p[0] = p[1]


def p_filter_node(p):
    '''filter_node : LBRACKET STRING COMMA filter_expressions RBRACKET'''

    p[0] = [p[2], *p[4]]


def p_filter_leaf(p):
    '''filter_leaf : LBRACKET STRING COMMA STRING COMMA filter_args RBRACKET'''

    p[0] = [p[2],p[4],*p[6]]


def p_filter_args(p):
    '''filter_args : filter_arg COMMA filter_args
                   | filter_arg'''
    
    if len(p) == 4:
        p[0] = [p[1]] + p[3]
    else:
        p[0] = [p[1]]


def p_filter_arg(p):
    '''filter_arg : STRING
                  | NUMBER
                  | expression'''

    p[0] = p[1]


def p_error(p):
    print("Error de sintaxis :DDDD")

parser = yacc.yacc()
