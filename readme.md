class Column:
    def __init__(
        self,
        name,
        datatype,
        is_pk=False,
        is_fk=False,
        ref_table=None,
        ref_column=None,
        nullable=True,
    ):
        self.name = name
        self.datatype = datatype
        self.is_pk = is_pk
        self.is_fk = is_fk
        self.ref_table = ref_table
        self.ref_column = ref_column
        self.nullable = nullable


class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.primary_keys = []
        self.pk_registry = set()
        self.generated_pk_values = []

    def add_column(self, column):
        self.columns.append(column)
        if column.is_pk:
            self.primary_keys.append(column.name)