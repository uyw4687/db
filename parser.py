import re
from lark import Lark

# print result using the parse result
def print_result(result):
    query_name = ' '.join(map(str.upper,re.findall("[a-z_]+_query",str(result))[0].split('_')[:-1]))
    print(PROMPT+f"'{query_name}' requested")

# initialize a parser
sql_parser = None
with open('grammar.lark') as file:
    sql_parser = Lark(file.read(), start="command", lexer="standard")

result = None
while True:
    print(PROMPT,end="")
    # if the parsing fails, parser will throw an exception so that
    # this program catches it and print 'Syntax error'
    try:
        queries = ''
        # get input till the last character of the input except the whitespaces is ;
        while True:
            queries += (input().strip()+'\n')
            if queries[-2]==';':
                break
        # split the input by ; and inject each query followed by ';' to the parser
        for query in queries.split(';')[:-1]:
            result = sql_parser.parse(query+';')
            print_result(result)

    except:
        print(PROMPT+'Syntax error')
