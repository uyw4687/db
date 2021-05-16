import json
from collections import OrderedDict
import lark
from bsddb3 import db

cdb = db.DB()
cdb.open('cdb.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

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
class SelectTableExistenceError(Exception):
    pass
class InsertTypeMismatchError(Exception):
    pass
class InsertColumnNonNullableError(Exception):
    pass
class InsertColumnExistenceError(Exception):
    pass
class InsertDuplicatePrimaryKeyError(Exception):
    pass
class WhereIncomparableError(Exception):
    pass
class WhereTableNotSpecified(Exception):
    pass
class WhereColumnNotExist(Exception):
    pass
class InsertReferentialIntegrityError(Exception):
    pass
class DeleteReferentialIntegrityPassed(Exception):
    pass
class SelectColumnResolveError(Exception):
    pass
class WhereAmbiguousReference(Exception):
    pass

class Transformer(lark.Transformer):
    def __init__(self):
        super().__init__()
        self.new_table = dict(cols=OrderedDict(), pks=set(), fors=OrderedDict(), invrefs=OrderedDict(), data=list())
        self.add_invs = list()
        self.queried_tbls = []
        self.tbl_cols = {}

    def clean(self):
        self.new_table = dict(cols=OrderedDict(), pks=set(), fors=OrderedDict(), invrefs=OrderedDict(), data=list())
        self.add_invs = list()
        self.queried_tbls = []
        self.tbl_cols = {}

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
        return items[0].lower()

    def create_table_query(self, items):
        new_table_name = items[2]
        if cdb.get(new_table_name.encode()) is not None:
            print("Create table has failed: table with the same name already exists")
            raise TableExistenceError
        
        self.new_table['pks'] = list(self.new_table['pks'])
        table_info = json.dumps(self.new_table)
        cdb.put(new_table_name.encode(), table_info.encode())

        for ref_tbl_name,ref_cols,from_cols in self.add_invs:
            ref_tbl = json.loads(cdb.get(ref_tbl_name.encode()),object_pairs_hook=OrderedDict)
            invrefs = ref_tbl['invrefs']
            for ref_col,from_col in zip(ref_cols,from_cols):
                if ref_col not in invrefs:
                    invrefs[ref_col] = [[new_table_name,from_col]]
                else:
                    invrefs[ref_col].append([ref_tbl_name,from_col])
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
                print("Create table has failed: column definition is duplicated")
                raise DuplicateColumnDefError

            col_type = col_info[1].children
            size = 0 # only for char
            if col_type[0] == 'char':
                size = int(col_type[2])
                if size < 1:
                    print("Char length should be over 0")
                    raise CharLengthError

            col_type = str(col_type[0])

            not_null = (len(col_info)==4)
            
            cols[col_name] = [col_type, size, not_null, False, False]
        
        else: # table_constraint_definition
            constraint_info = item.children[0]
            if constraint_info.data == 'primary_key_constraint':
                pks = self.new_table['pks']
                if len(pks)!=0:
                    print("Create table has failed: primary key definition is duplicated")
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

                if len(here_cols)!=len(ref_cols):
                    print("Create table has failed: foreign key references wrong type")
                    raise ReferenceTypeError
                    
                if (ref_tbl:=cdb.get(ref_tbl_name.encode())) is None:
                    print("Create table has failed: foreign key references non existing table")
                    raise ReferenceTableExistenceError
                ref_tbl = json.loads(ref_tbl,object_pairs_hook=OrderedDict)
                ref_pks = ref_tbl['pks']

                fors = self.new_table['fors']
                done = [False]*len(ref_pks)
                for here_col_name, ref_col_name in zip(here_cols, ref_cols):
                    if (ref_col:=ref_tbl['cols'].get(ref_col_name)) is None:
                        print("Create table has failed: foreign key references non existing column")
                        raise ReferenceColumnExistenceError

                    here_col = cols[here_col_name]
                    if ref_col[:2]!=here_col[:2]:
                        print("Create table has failed: foreign key references wrong type")
                        raise ReferenceTypeError

                    if ref_col_name not in ref_pks:
                        print("Create table has failed: foreign key references non primary key column")
                        raise ReferenceNonPrimaryKeyError
                    done[ref_pks.index(ref_col_name)] = True
                    fors[here_col_name] = [ref_tbl_name, ref_col_name]
                    cols[here_col_name][4] = True

                for x in done:
                    if not x:
                        print("Create table has failed: foreign key references non primary key column")
                        raise ReferenceNonPrimaryKeyError

                self.add_invs.append([ref_tbl_name,ref_cols,here_cols])

        return item

    def drop_table_query(self, items):
        tbl_name = items[2]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print('No such table')
            raise NoSuchTable

        tbl = json.loads(tbl,object_pairs_hook=OrderedDict)
        if len(tbl['invrefs'])!=0:
            print(f"Drop table has failed: '{tbl_name}' is referenced by other table")
            raise DropReferencedTableError
        
        cdb.delete(tbl_name.encode())
        print(f"'{tbl_name}' table is dropped")
        return items
    
    def desc_query(self, items):
        tbl_name = items[1]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print("No such table")
            raise NoSuchTable
        
        tbl = json.loads(tbl,object_pairs_hook=OrderedDict)
        print("-------------------------------------------------")
        print(f'table_name [{tbl_name}]')
        print(''.join(map(lambda x:str.ljust(x,20),['column_name', 'type', 'null', 'key'])))
        for col_name, col_info in tbl['cols'].items():
            c_type,size,not_null,pk,fk = col_info
            if c_type=='char':
                c_type = f'{c_type}({size})'
            else:
                c_type+='\t'
            key = ''
            if pk:
                key+='PRI'
            if fk:
                if len(key)!=0:
                    key+='/'
                key+='FOR'
            print(''.join(map(lambda x:str.ljust(x,20),[col_name, c_type, 'Y' if not_null else 'N', key])))
        print("-------------------------------------------------")

        return items

    def show_tables_query(self, items):
        cursor = cdb.cursor()
        print("----------------")
        while (kv:=cursor.next()) is not None:
            tbl_name,_ = kv
            print(tbl_name.decode())
        print("----------------")
        return items

    def comparable_value(self, items):
        return items[0]

    def value(self, items):
        return items[0]
        
    def value_list(self, items):
        return items[2:-1]

    def insert_columns_and_sources(self, items):
        return items

    def insert_query(self, items):
        tbl_name = items[2]
        cols=values=None
        if len(items[3])==1:
            values=items[3][0]
        else:
            cols,values=items[3]
            cols = cols.children[1:-1]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print("No such table")
            raise NoSuchTable
        tbl = json.loads(tbl,object_pairs_hook=OrderedDict)

        if cols is not None:
            for col_name in cols:
                if col_name not in tbl['cols']:
                    print(f"Insertion has failed: '{col_name}' does not exist")
                    raise InsertColumnExistenceError

            if len(cols)!=len(values):
                print("Insertion has failed: Types are not matched")
                raise InsertTypeMismatchError

            order = {x:i for i,x in enumerate(tbl['cols'])}
            values_ordered = [None]*len(tbl['cols'])
            for col_name, value in zip(cols, values):
                values_ordered[order[col_name]] = value
            values = values_ordered
            
        else:
            if len(values)!=len(tbl['cols']):
                print("Insertion has failed: Types are not matched")
                raise InsertTypeMismatchError

        to_type = {'INT':'int', 'STR':'char', 'DATE':'date'}
        for i,(value, (col_name,col_info)) in enumerate(zip(values, tbl['cols'].items())):
            c_type,size,not_null,pk,fk = col_info
            if (value is None) or (value.type=='NULL'):
                value = None
                if not_null:
                    print(f"Insertion has failed: '{col_name}' is not nullable")
                    raise InsertColumnNonNullableError
            elif to_type[value.type]!=c_type:
                print("Insertion has failed: Types are not matched")
                raise InsertTypeMismatchError
            else:
                value = value.value

            if (value is not None) and (c_type=='char'):
                value = values[i][1:-1][:size]
            if pk:
                for record in tbl['data']:
                    if record[i]==value:
                        print("Insertion has failed: Primary key duplication")
                        raise InsertDuplicatePrimaryKeyError
            if fk:
                ref_tbl_name,ref_col_name = tbl['fors'][col_name]
                ref_tbl=cdb.get(ref_tbl_name.encode())
                ref_tbl = json.loads(ref_tbl,object_pairs_hook=OrderedDict)
                ref_ind = list(ref_tbl['cols']).index(ref_col_name)
                present = False
                for record in ref_tbl['data']:
                    if record[ref_ind]==value:
                        present = True
                        break
                if not present:
                    print("Insertion has failed: Referential integrity violation")
                    raise InsertReferentialIntegrityError
            values[i] = value

        tbl['data'].append(values)
        table_info = json.dumps(tbl)
        cdb.put(tbl_name.encode(), table_info.encode())
        print("The row is inserted")

        return items
    
    def comp_operand(self, items):
        if len(items)==1:
            return items[0]
        return items

    def get_val(self, record, tbl_col):
        tbl,col = tbl_col if len(tbl_col)==2 else (None,tbl_col)
        if (tbl is not None) and (tbl not in self.tbl_names):
            print("Where clause try to reference tables which are not specified")
            raise WhereTableNotSpecified
        
        if tbl is not None:
            if (tbl,col) not in self.tbl_cols:
                print("Where clause try to reference non existing column")
                raise WhereColumnNotExist
        else:
            col_cnt = 0
            for tbl_name,col_name in self.tbl_cols:
                if col_name==col:
                    col_cnt+=1; tbl=tbl_name 
            if col_cnt>1:
                print("Where clause contains ambiguous reference")
                raise WhereAmbiguousReference
            if col_cnt==0:
                print("Where clause try to reference non existing column")
                raise WhereColumnNotExist

        val = record[list(self.tbl_cols).index((tbl,col))]
        if val is None: return None
        return [self.tbl_cols[(tbl,col)][0], val]

    def check(self, record, cond):
        if cond.data=='boolean_expr':
            result = False
            for boolean_term in cond.children[::2]:
                if self.check(record, boolean_term) is True:
                    return True
                if self.check(record, boolean_term) is None:
                    result = None
            return result

        elif cond.data=='boolean_term':
            result = True
            for boolean_factor in cond.children[::2]:
                if self.check(record, boolean_factor) is False:
                    return False
                if self.check(record, boolean_factor) is None:
                    result = None
            return result

        elif cond.data=='boolean_factor':
            if len(cond.children)==1:
                return self.check(record, cond.children[0].children[0])
            else:
                result = self.check(record, cond.children[1].children[0])
                if result is None:
                    return None
                else:
                    return not result

        elif cond.data=='parenthesized_boolean_expr':
            return self.check(record, cond.children[1])

        elif cond.data=='predicate':
            return self.check(record, cond.children[0])

        elif cond.data=='comparison_predicate':
            lo = cond.children[0]
            op = cond.children[1].value
            ro = cond.children[2]
            if type(cond.children[0])!=lark.lexer.Token:
                lo = self.get_val(record, cond.children[0])
                if lo is None:
                    return None
            else:
                lo=[lo.type.lower(),lo.value]   
            if type(cond.children[2])!=lark.lexer.Token:
                ro = self.get_val(record, cond.children[2])
                if lo is None:
                    return None
            else:
                ro=[ro.type.lower(),ro.value]

            if lo[0]=='str': lo[0],lo[1]='char',lo[1][1:-1]
            if ro[0]=='str': ro[0],ro[1]='char',ro[1][1:-1]
            if lo[0]!=ro[0]:
                print("Where clause try to compare incomparable values")
                raise WhereIncomparableError

            lo,ro = lo[1],ro[1]
            if (op=='<'): return lo<ro
            elif (op=='>'): return lo>ro
            elif (op=='='): return lo==ro
            elif (op=='>='): return lo>=ro
            elif (op=='<='): return lo<=ro
            else: return lo!=ro

        elif cond.data=='null_predicate':
            tbl_col = cond.children[:-1]
            if len(tbl_col)==1: tbl_col=tbl_col[0]
            val = (self.get_val(record, tbl_col) is None)
            if len(cond.children[-1].children)==3: val = not val
            return val
    
    def inv_ref(self, record, cols, invrefs):
        for col,col_name in zip(record,cols):
            if (ref_info:=invrefs.get(col_name)) is None:
                continue
            for ref_tbl_name,ref_col_name in ref_info:
                ref_tbl = json.loads(cdb.get(ref_tbl_name.encode()),object_pairs_hook=OrderedDict)
                ref_ind = list(ref_tbl['cols']).index(ref_col_name)
                for ref_rec in ref_tbl['data']:
                    if ref_rec[ref_ind]==col:
                        return True
        return False

    def delete_query(self, items):
        tbl_name = items[2]

        if (tbl:=cdb.get(tbl_name.encode())) is None:
            print('No such table')
            raise NoSuchTable
        tbl=json.loads(tbl,object_pairs_hook=OrderedDict)

        if len(items)==3:
            print(f"{len(tbl['data'])} row(s) are deleted")
            tbl['data']=[]
            table_info = json.dumps(tbl)
            cdb.put(tbl_name.encode(), table_info.encode())
        
        else:
            self.queried_tbls = [tbl_name]
            self.tbl_cols = {(tbl_name,col_name):col_info for col_name,col_info in tbl['cols'].items()}

            new_data = []
            cond = items[3].children[1]
            count = 0
            inv_refs = 0
            for record in tbl['data']:
                if self.check(record, cond) is True:
                    if self.inv_ref(record,tbl['cols'],tbl['invrefs']):
                        inv_refs+=1
                    else:
                        count+=1; continue
                new_data.append(record)

            tbl['data']=new_data
            table_info = json.dumps(tbl)
            cdb.put(tbl_name.encode(), table_info.encode())
            print(f"{count} row(s) are deleted")
            if inv_refs!=0:
                print(f"{inv_refs} row(s) are not deleted due to referential integrity")
                raise DeleteReferentialIntegrityPassed

        return items

    def select_list(self, items):
        return items

    def table_expression(self, items):
        if len(items)==2:
            items[1] = items[1].children[1]
        return items

    def from_clause(self, items):
        return items[1]

    def table_reference_list(self, items):
        return items

    def referred_table(self, items):
        return items[0]

    def add_recs(self, sel_recs, tbls, curr):
        if len(tbls)==0:
            sel_recs.append(curr)
            return
        for record in tbls[0]['data']:
            self.add_recs(sel_recs, tbls[1:], curr+record)

    def selected_column(self, items):
        return items

    def select_query(self, items):
        tbl_names = items[2][0]
        sel_tbl_cols = {}
        tbls = []
        for tbl_name in tbl_names:
            if (tbl:=cdb.get(tbl_name.encode())) is None:
                print(f"Selection has failed: '{tbl_name}' does not exist")
                raise SelectTableExistenceError
            tbl = json.loads(tbl)
            tbls.append(tbl)
            for col_name,col_info in tbl['cols'].items():
                sel_tbl_cols[(tbl_name,col_name)] = col_info
        
        sel_recs = []
        self.add_recs(sel_recs,tbls,[])
        
        if len(items[2])!=1:
            self.queried_tbls = tbl_names
            self.tbl_cols = sel_tbl_cols

            new_sel_recs = []
            for record in sel_recs:
                if self.check(record, items[2][1]) is not True:
                    continue
                new_sel_recs.append(record)
            sel_recs = new_sel_recs

        if len(items[1])!=0:
            sel_order = []
            renames = []
            for i,sel_tbl_col in enumerate(items[1]):
                if len(sel_tbl_col)==2:
                    sel_tbl,sel_col=sel_tbl_col
                elif len(sel_tbl_col)>=3 and type(sel_tbl_col[-2])==lark.lexer.Token:
                    if len(sel_tbl_col)==3:
                        sel_tbl,(sel_col,_,sel_as)=None,sel_tbl_col
                    else:
                        sel_tbl,sel_col,_,sel_as=sel_tbl_col
                    renames.append((i,sel_as))
                else:
                    sel_tbl=None; sel_col=sel_tbl_col[0]

                if sel_tbl is not None:
                    if (sel_tbl not in tbl_names) or ((sel_tbl,sel_col) not in sel_tbl_cols):
                        print(f"Selection has failed: fail to resolve '{sel_tbl+'.'+sel_col}'")
                        raise SelectColumnResolveError
                    sel_order.append(list(sel_tbl_cols).index((sel_tbl,sel_col)))
                else:
                    col_cnt = 0
                    sel_tbl = None
                    for tbl_name,col_name in sel_tbl_cols:
                        if col_name==sel_col:
                            col_cnt+=1; sel_tbl = tbl_name
                    if col_cnt!=1:
                        print(f"Selection has failed: fail to resolve '{sel_col}'")
                        raise SelectColumnResolveError
                    else:
                        sel_order.append(list(sel_tbl_cols).index((sel_tbl,sel_col)))

            sel_tbl_cols = list(sel_tbl_cols)
            sel_tbl_cols = [sel_tbl_cols[i] for i in sel_order]
            for i,sel_as in renames:
                sel_tbl_cols[i] = [sel_tbl_cols[i][0],sel_as]
            new_sel_recs = []
            for record in sel_recs:
                new_sel_recs.append([record[i] for i in sel_order])
            sel_recs = new_sel_recs

        div = "+-"
        lens = []
        ncols = len(sel_tbl_cols)
        for i,(_,col_name) in enumerate(sel_tbl_cols):
            lens.append(len(col_name))
            div += '-'*len(col_name)
            if i!=ncols-1:
                div += "-+-"
        div += "-+"

        print(div)
        print("| ",end=""); print(" | ".join([col_name for (_,col_name) in sel_tbl_cols]), end=""); print(" |")
        print(div)
        for record in sel_recs:
            print("| ",end="")
            for col,col_space in zip(record,lens):
                print(str(col).ljust(col_space),end=' | ')
            print()
        print(div)
            
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
                transformer.clean()
                msg = transformer.transform(tree)[0]
            except Exception as e:
                if isinstance(e, lark.exceptions.UnexpectedInput):
                    print(prompt + "Syntax error")
                break