import pandas as pd
from models import Table, Column


def parse_schema(file_path):
    df = pd.read_excel(file_path)

    tables = {}

    for _, row in df.iterrows():
        table_name = row["Table"]

        if table_name not in tables:
            tables[table_name] = Table(table_name)

        column = Column(
            name=row["Column"],
            datatype=row["DataType"],
            is_pk=str(row["PK"]).upper() == "Y",
            is_fk=str(row["FK"]).upper() == "Y",
            ref_table=row.get("ReferenceTable"),
            ref_column=row.get("ReferenceColumn"),
            nullable=str(row.get("Nullable", "Y")).upper() == "Y",
        )

        tables[table_name].add_column(column)

    return tables