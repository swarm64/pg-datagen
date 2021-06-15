
import json
import re

from collections import OrderedDict, namedtuple
from dataclasses import dataclass

from pglast.parser import parse_sql_json


COMMENT_RE = re.compile(r"--\s*(.+)")


@dataclass
class Column:
    gen: str
    not_null: bool
    args: list


class Schema:
    def __init__(self, path):
        with open(path, 'r') as schema_file:
            self.raw_schema = schema_file.read()
            self.schema = json.loads(parse_sql_json(self.raw_schema))
        assert self.schema

    def _get_column_gen(self, column):
        column_location_start = column['location']
        column_location_end = self.raw_schema.find('\n', column_location_start)
        column_gen = None
        raw = self.raw_schema[column_location_start:column_location_end]
        comment = COMMENT_RE.search(raw)
        if comment:
            comment = comment.groups()[0]
            print(comment)
            if comment.find('gen:') >= 0:
                column_gen = comment.split('gen:')[1].strip()

        # The column generator might have been set from a comment
        # directly, thus do not overwrite
        if not column_gen:
            column_type_def = column['typeName']
            column_gen = None
            for column_type in column_type_def['names']:
                assert not column_gen, 'column_gen already set'

                column_gen = column_type['String']['str']

                if column_gen == 'pg_catalog':
                    column_gen = None
                    continue

                if column_gen not in (
                    'varchar', 'text', 'int2', 'int4', 'int8',
                    'timestamp', 'date', 'numeric', 'bpchar'):
                    raise ValueError(f'Unsupported column generator: {column_gen}')

        return column_gen

    @classmethod
    def _get_column_gen_args(cls, column_gen, column):
        typmods = column['typeName'].get('typmods', [])
        if column_gen == 'varchar' or column_gen == 'bpchar':
            # Varchar can have an upper limit
            for typmod in typmods:
                if 'Integer' in typmod['A_Const']['val']:
                    return [typmod['A_Const']['val']['Integer']['ival']]

        elif column_gen == 'numeric':
            # Numerics must have scale and precision
            precision = typmods[0]['A_Const']['val']['Integer']['ival']
            scale = typmods[1]['A_Const']['val']['Integer']['ival']
            return [precision, scale]

        return []


    def parse_create_table(self):
        columns = OrderedDict()

        for stmt in self.schema['stmts']:
            create_stmt = stmt.get('stmt', {}).get('CreateStmt', {})
            if create_stmt:
                schema_name = create_stmt['relation']['schemaname']
                table_name = create_stmt['relation']['relname']

                for column in create_stmt['tableElts']:
                    column = column['ColumnDef']
                    column_name = column['colname']
                    column_gen = self._get_column_gen(column)
                    column_gen_args = Schema._get_column_gen_args(column_gen, column)

                    constraints = column.get('constraints', [])
                    not_null = False
                    for constraint in constraints:
                        if constraint['Constraint']['contype'] == 'CONSTR_NOTNULL':
                            not_null = True

                    assert column_gen, f'Column generator empty, column: {column}'
                    column = Column(column_gen, not_null, column_gen_args)
                    columns[column_name] = column

            alter_table_stmt = stmt.get('stmt', {}).get('AlterTableStmt', {})
            if alter_table_stmt:
                alter_table_cmd = alter_table_stmt['cmds'][0]['AlterTableCmd']
                if alter_table_cmd['subtype'] == 'AT_AddConstraint':
                    alter_table_def = alter_table_cmd['def']['Constraint']
                    if alter_table_def['contype'] == 'CONSTR_PRIMARY':
                        assert len(alter_table_def['keys']) == 1, \
                            'Multiple keys in constraint not supported'

                        column_name = alter_table_def['keys'][0]['String']['str']
                        columns[column_name].not_null = True

        return columns
