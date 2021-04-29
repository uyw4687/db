import json
import lark
from bsddb3 import db

cdb = db.DB()
cdb.open('cdb.db', dbtype=db.DB_HASH)

class TableExistenceError(Exception):
    pass
class DuplicateColumnDefError(Exception):
    pass
class CharLengthError(Exception):
    pass
class DuplicatePrimaryKeyDefError(Exception):
    pass
class NonExistingColumnDefError(Exception):
    pass
class ReferenceTableExistenceError(Exception):
    pass
class ReferenceColumnExistenceError(Exception):
    pass
class ReferenceNonPrimaryKeyError(Exception):
    pass
class ReferenceTypeError(Exception):
    pass
class NoSuchTable(Exception):
    pass
class DropReferencedTableError(Exception):
    pass

class Transformer(lark.Transformer):
    def __init__(self):
        super().__init__()
        self.new_table = dict(cols=dict(), pks=set(), fors=dict(), invrefs=list())
        self.add_invs = set()

    def command(self, items):
        if not isinstance(items[0], list):
            cdb.close()
            exit()
        return items[0]

    def query_list(self, items):
        return items

    def table_name(self, items):
        return items[0].lower()
    
    def column_name(self, items):
        return items[0]

    def create_table_query(self, items):
        new_table_name = items[2]
        if cdb.get(new_table_name.encode()) is not None:
            print('Create table has failed: table with the same name already exists')
            raise TableExistenceError
        
        self.new_table['pks'] = list(self.new_table['pks'])
        table_info = json.dumps(self.new_table)
        cdb.put(new_table_name.encode(), table_info.encode())

        for ref_tbl_name in self.add_invs:
            ref_tbl = json.loads(cdb.get(ref_tbl_name.encode()))
            ref_tbl['invrefs'].append(new_table_name)
            cdb.put(ref_tbl_name.encode(), json.dumps(ref_tbl).encode())

        print(f"'{new_table_name}' table is created")
        return items

    def table_element(self, items):
        item = items[0]
        cols = self.new_table['cols']

        if item.data == 'column_definition':
            col_info = item.children
            col_name = str(col_info[0]).lower()
            if col_name in cols:
                print('Create table has failed: column definition is duplicated')
                raise DuplicateColumnDefError

            col_type = col_info[1].children
            size = 0 # only for char
            if col_type[0] == 'char':
                size = int(col_type[2])
                if size < 1:
                    print('Char length should be over 0')
                    raise CharLengthError

            col_type = str(col_type[0])

            not_null = (len(col_info)==4)
            
            cols[col_name] = [col_type, size, not_null, False, False]
        
        else: # table_constraint_definition
            constraint_info = item.children[0]
            if constraint_info.data == 'primary_key_constraint':
                pks = self.new_table['pks']
                if len(pks)!=0:
                    print('Create table has failed: primary key definition is duplicated')
                    raise DuplicatePrimaryKeyDefError
                
                pk_info = constraint_info.children[2].children[1:-1]
                for col_name in pk_info:
                    col_name = str(col_name).lower()
                    if col_name not in cols:
                        print(f"Create table has failed: '{col_name}' does not exists in column definition")
                        raise NonExistingColumnDefError
                    cols[col_name][2] = True
                    cols[col_name][3] = True
                    pks.add(col_name)

            else: # referential_constraint
                constraint_info = constraint_info.children
                here_cols = list(map(str.lower,constraint_info[2].children[1:-1]))
                ref_cols = list(map(str.lower,constraint_info[5].children[1:-1]))
                ref_tbl_name = constraint_info[4]

                for col_name in here_cols:
                    if col_name not in cols:
                        print(f"Create table has failed: '{col_name}' does not exists in column definition")
                        raise NonExistingColumnDefError

                if (ref_tbl:=cdb.get(ref_tbl_name.encode())) is None:
                    print('Create table has failed: foreign key references non existing table')
                    raise ReferenceTableExistenceError
                ref_tbl = json.loads(ref_tbl)
                ref_cols = ref_tbl['cols']
                ref_pks = ref_tbl['pks']

                if len(here_cols)!=len(ref_cols):
                    print('Create table has failed: foreign key references wrong type')
                    raise ReferenceTypeError

                fors = self.new_table['fors']
                done = [False]*len(ref_pks)
                for here_col_name, ref_col_name in zip(here_cols, ref_cols):
                    if (ref_col:=ref_cols.get(ref_col_name)) is None:
                        print('Create table has failed: foreign key references non existing column')
                        raise ReferenceColumnExistenceError

                    here_col = cols[here_col_name]
                    if ref_col[:2]!=here_col[:2]:
                        print('Create table has failed: foreign key references wrong type')
                        raise ReferenceTypeError

                    if ref_col_name not in ref_pks:
                        print('Create table has failed: foreign key references non primary key column')
                        raise ReferenceNonPrimaryKeyError
                    done[ref_pks.index(ref_col_name)] = True
                    fors[here_col_name] = [ref_tbl_name, ref_col_name]
                    cols[here_col_name][4] = True

                for x in done:
                    if not x:
                        print('Create table has failed: foreign key references non primary key column')
                        raise ReferenceNonPrimaryKeyError

                self.add_invs.add(ref_tbl_name)

        return item

    def drop_table_query(self, items):
        tbl_name = items[2]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print('No such table')
            raise NoSuchTable

        tbl = json.loads(tbl)
        if len(tbl['invrefs'])!=0:
            print(f"Drop table has failed: '{tbl_name}' is referenced by other table")
            raise DropReferencedTableError
        
        cdb.delete(tbl_name.encode())
        print(f"'{tbl_name}' table is dropped")
        return items
    
    def desc_query(self, items):
        tbl_name = items[1]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print('No such table')
            raise NoSuchTable
        
        tbl = json.loads(tbl)
        print('-------------------------------------------------')
        print(f'table_name [{tbl_name}]')
        print(''.join(map(lambda x:str.ljust(x,20),['column_name', 'type', 'null', 'key'])))
        for col_name, col_info in tbl['cols'].items():
            type,size,null,pk,fk = col_info
            if type=='char':
                type = f'{type}({size})'
            else:
                type+='\t'
            key = ''
            if pk:
                key+='PRI'
            if fk:
                if len(key)!=0:
                    key+='/'
                key+='FOR'
            print(''.join(map(lambda x:str.ljust(x,20),[col_name, type, 'Y' if null else 'N', key])))
        print('-------------------------------------------------')

        return items

    def show_tables_query(self, items):
        cursor = cdb.cursor()
        print('----------------')
        while (kv:=cursor.next()) is not None:
            tbl_name,_ = kv
            print(tbl_name.decode())
        print('----------------')

        return items

def input_queries(prompt):
    s = input(prompt)
    if not s.strip():
        return []
    while not s.rstrip().endswith(';'):
        s += '\n' + input()
    return [x + ';' for x in s.split(';')[:-1]]

if __name__ == "__main__":
    prompt = "DB_example> "

    with open('grammar.lark') as file:
        parser = lark.Lark(file.read(), start="command", lexer='standard')
    transformer = Transformer()

    while True:
        for query in input_queries(prompt):
            try:
                tree = parser.parse(query)
                msg = transformer.transform(tree)[0]
            except Exception as e:
                if isinstance(e, lark.exceptions.UnexpectedInput):
                    print(prompt + "Syntax error")
                break