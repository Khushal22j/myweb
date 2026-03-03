"""
Schema Parser — reads Collibra-style Excel schema files.

Supported sheet structures:
  - Collibra export format (Table Name / Attribute Name / Data Type / etc.)
  - Generic schema sheet with configurable column headers
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import pandas as pd


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ColumnDef:
    name: str
    data_type: str = "VARCHAR"
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    ref_table: Optional[str] = None
    ref_column: Optional[str] = None
    is_unique: bool = False
    default_value: Any = None
    description: Optional[str] = None
    allowed_values: Optional[List[str]] = None
    min_value: Any = None
    max_value: Any = None
    is_composite_key: bool = False
    composite_key_group: Optional[str] = None

    @property
    def is_auto_increment(self) -> bool:
        dt = (self.data_type or "").upper()
        return any(x in dt for x in ["SERIAL","SEQUENCE","AUTOINCREMENT","IDENTITY","AUTO_INCREMENT"])


@dataclass
class TableDef:
    name: str
    schema: str = "dbo"
    columns: List[ColumnDef] = field(default_factory=list)
    description: Optional[str] = None

    @property
    def primary_keys(self) -> List[ColumnDef]:
        return [c for c in self.columns if c.is_primary_key]

    @property
    def foreign_keys(self) -> List[ColumnDef]:
        return [c for c in self.columns if c.is_foreign_key]

    @property
    def composite_key_groups(self) -> Dict[str, List[ColumnDef]]:
        groups: Dict[str, List[ColumnDef]] = {}
        for col in self.columns:
            if col.is_composite_key and col.composite_key_group:
                groups.setdefault(col.composite_key_group, []).append(col)
        return groups


# ---------------------------------------------------------------------------
# Header synonym mappings (case-insensitive)
# ---------------------------------------------------------------------------

_HEADER_MAP = {
    # ── Table name (sheet name is used; explicit table column optional) ──
    "table": "table",
    "table name": "table",
    "table_name": "table",
    "object name": "table",
    "object type": "table",
    "entity": "table",
    "entity name": "table",
    "dataset": "table",
    "domain object": "table",
    "business object": "table",

    # ── Column / Attribute name ────────────────────────────────────────
    "name": "column",               # Collibra: your exact header
    "column": "column",
    "column name": "column",
    "column_name": "column",
    "attribute": "column",
    "attribute name": "column",
    "field": "column",
    "field name": "column",
    "property": "column",
    "element": "column",
    "element name": "column",

    # ── Sequence / Order ───────────────────────────────────────────────
    "sequence order": "sequence",   # Collibra: your exact header
    "sequence_order": "sequence",
    "sequence": "sequence",
    "order": "sequence",
    "ordinal": "sequence",
    "ordinal position": "sequence",
    "position": "sequence",
    "col order": "sequence",
    "column order": "sequence",
    "sort order": "sequence",
    "column position": "sequence",

    # ── Data type ──────────────────────────────────────────────────────
    "data type": "data_type",       # Collibra: your exact header
    "data_type": "data_type",
    "datatype": "data_type",
    "type": "data_type",
    "col type": "data_type",
    "column type": "data_type",
    "technical data type": "data_type",
    "technical type": "data_type",
    "attribute type": "data_type",
    "physical type": "data_type",

    # ── Length ─────────────────────────────────────────────────────────
    "length": "length",             # Collibra: your exact header
    "max length": "length",
    "size": "length",
    "char length": "length",
    "character maximum length": "length",
    "max size": "length",

    # ── Precision ──────────────────────────────────────────────────────
    "precision": "precision",
    "numeric precision": "precision",
    "total digits": "precision",

    # ── Scale ──────────────────────────────────────────────────────────
    "scale": "scale",               # Collibra: your exact header
    "numeric scale": "scale",
    "decimal places": "scale",
    "decimal scale": "scale",

    # ── Nullable ───────────────────────────────────────────────────────
    "nullable": "nullable",
    "null": "nullable",
    "is null": "nullable",
    "is nullable": "nullable",
    "nullability": "nullable",
    "mandatory": "nullable",
    "required": "nullable",
    "not null": "nullable",
    "is required": "nullable",
    "is mandatory": "nullable",
    "optional": "nullable",

    # ── Primary key ────────────────────────────────────────────────────
    "is primary key?": "primary_key",   # Collibra: your exact header
    "is primary key": "primary_key",
    "primary key": "primary_key",
    "primary key?": "primary_key",
    "pk": "primary_key",
    "is pk": "primary_key",
    "is pk?": "primary_key",
    "pk?": "primary_key",
    "key type": "key_type",
    "key": "primary_key",

    # ── Foreign key ────────────────────────────────────────────────────
    "is foreign key?": "foreign_key",
    "is foreign key": "foreign_key",
    "foreign key": "foreign_key",
    "foreign key?": "foreign_key",
    "fk": "foreign_key",
    "is fk": "foreign_key",
    "is fk?": "foreign_key",
    "fk?": "foreign_key",

    # ── FK references ──────────────────────────────────────────────────
    "reference table": "ref_table",
    "ref table": "ref_table",
    "referenced table": "ref_table",
    "fk table": "ref_table",
    "parent table": "ref_table",
    "references": "ref_table",
    "fk reference": "ref_table",

    "reference column": "ref_column",
    "ref column": "ref_column",
    "referenced column": "ref_column",
    "fk column": "ref_column",
    "parent column": "ref_column",
    "references column": "ref_column",

    # ── Unique ─────────────────────────────────────────────────────────
    "unique": "unique",
    "is unique": "unique",
    "is unique?": "unique",
    "unique?": "unique",
    "unique constraint": "unique",
    "is unique key": "unique",

    # ── Default value ──────────────────────────────────────────────────
    "default": "default_value",
    "default value": "default_value",
    "default_value": "default_value",

    # ── Allowed / enumerated values ────────────────────────────────────
    "allowed values": "allowed_values",
    "enum": "allowed_values",
    "enumeration": "allowed_values",
    "valid values": "allowed_values",
    "lookup values": "allowed_values",
    "domain": "allowed_values",
    "domain values": "allowed_values",
    "code list": "allowed_values",
    "permissible values": "allowed_values",
    "value list": "allowed_values",

    # ── Min / Max ──────────────────────────────────────────────────────
    "min": "min_value",
    "minimum": "min_value",
    "min value": "min_value",
    "max": "max_value",
    "maximum": "max_value",
    "max value": "max_value",

    # ── Description / Notes ────────────────────────────────────────────
    "description": "description",
    "desc": "description",
    "comment": "description",
    "definition": "description",
    "notes": "description",
    "remarks": "description",
    "business description": "description",
    "technical description": "description",

    # ── Composite key ──────────────────────────────────────────────────
    "composite key": "composite_key",
    "is composite key": "composite_key",
    "is composite key?": "composite_key",
    "composite key?": "composite_key",
    "composite key group": "composite_key_group",
    "key group": "composite_key_group",
    "constraint name": "composite_key_group",
}


def _norm_header(h: str) -> str:
    return str(h).lower().strip()


def _map_header(h: str) -> Optional[str]:
    return _HEADER_MAP.get(_norm_header(h))


def _is_truthy(val) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    s = str(val).strip().upper()
    return s in ("YES","Y","TRUE","1","X","PK","FK","UK","UNIQUE","PRIMARY","FOREIGN","MANDATORY","NOT NULL","NN")


def _parse_allowed(val) -> Optional[List[str]]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    # Try comma / semicolon / pipe separated
    for sep in ["|", ";", ","]:
        if sep in s:
            return [v.strip() for v in s.split(sep) if v.strip()]
    return [s]


def _safe_int(val) -> Optional[int]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(float(str(val).strip()))
    except Exception:
        return None


def _safe_any(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def _parse_ref(ref_str: str) -> tuple:
    """Parse 'table.column' or 'table(column)' references."""
    if not ref_str:
        return None, None
    ref_str = str(ref_str).strip()
    m = re.match(r"^([^.(]+)[.(]([^.)]+)[)]?$", ref_str)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    if "." in ref_str:
        parts = ref_str.split(".", 1)
        return parts[0].strip(), parts[1].strip()
    return ref_str, None


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

class SchemaParser:

    def parse_excel(self, path: str) -> List[TableDef]:
        """
        Parse a Collibra-style Excel schema file.
        Each sheet is attempted; sheets with recognizable schema data are parsed.
        """
        xl = pd.read_excel(path, sheet_name=None, header=None, dtype=str)
        all_tables: Dict[str, TableDef] = {}

        for sheet_name, raw_df in xl.items():
            if raw_df.empty:
                continue
            tables = self._parse_sheet(raw_df, sheet_name)
            for t in tables:
                key = f"{t.schema}.{t.name}".lower()
                if key not in all_tables:
                    all_tables[key] = t
                else:
                    # Merge columns from duplicate definitions
                    existing_names = {c.name.lower() for c in all_tables[key].columns}
                    for c in t.columns:
                        if c.name.lower() not in existing_names:
                            all_tables[key].columns.append(c)

        if not all_tables:
            raise ValueError(
                "No schema tables could be parsed from the Excel file.\n"
                "Please ensure the file has columns: Table, Column, Data Type, "
                "Primary Key, Foreign Key, etc.\n"
                "See sample_schemas/sample_schema.xlsx for a reference template."
            )

        return list(all_tables.values())

    def _parse_sheet(self, df: pd.DataFrame, sheet_name: str) -> List[TableDef]:
        """Detect header row, map columns, extract table definitions."""
        # Find header row (first row where ≥2 cells match known headers)
        header_row_idx = None
        for i in range(min(10, len(df))):
            row = df.iloc[i]
            matches = sum(1 for cell in row if _map_header(str(cell or "")) is not None)
            if matches >= 2:
                header_row_idx = i
                break

        if header_row_idx is None:
            return []

        headers = [str(h) if h is not None else "" for h in df.iloc[header_row_idx]]

        # Build mapped: last writer wins per field (handles duplicate semantics)
        # Special case: if both "name" and a more specific column header exist,
        # prefer the more specific one for "column" mapping.
        mapped: Dict[str, int] = {}
        for idx, h in enumerate(headers):
            field = _map_header(h)
            if field is not None:
                # "name" alone maps to "column" — but only keep it if nothing
                # more specific has already claimed "column"
                if h.strip().lower() == "name" and "column" in mapped:
                    continue
                mapped[field] = idx

        # Need at minimum a column name
        if "column" not in mapped:
            return []

        data = df.iloc[header_row_idx + 1:].reset_index(drop=True)
        data = data.dropna(how="all")

        # Collect rows with sequence numbers so we can sort per table
        parsed_rows = []
        # Sheet name is the table name — strip schema prefix if present
        if "." in sheet_name:
            _schema_default, _table_default = sheet_name.split(".", 1)
        else:
            _schema_default, _table_default = "dbo", sheet_name

        tables: Dict[str, TableDef] = {}

        for _, row in data.iterrows():
            def get(field_name, _row=row):
                idx = mapped.get(field_name)
                if idx is None:
                    return None
                val = _row.iloc[idx] if idx < len(_row) else None
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                s = str(val).strip()
                return s if s and s.lower() not in ("nan","none","null","") else None

            # Table name: always use sheet name.
            # An optional explicit "Table" column is only used if the schema
            # intentionally stores multiple tables on one sheet.
            explicit_table = get("table") if "table" in mapped else None
            if explicit_table:
                tbl_raw = explicit_table.strip()
                if "." in tbl_raw:
                    schema_part, table_part = tbl_raw.split(".", 1)
                else:
                    schema_part, table_part = _schema_default, tbl_raw
            else:
                schema_part, table_part = _schema_default, _table_default

            tbl_key = f"{schema_part}.{table_part}".lower()

            if tbl_key not in tables:
                tables[tbl_key] = TableDef(name=table_part, schema=schema_part)

            t = tables[tbl_key]

            # Column name — from "Name" column
            col_name = get("column")
            if not col_name:
                continue

            # Sequence order — used to sort columns correctly
            seq = _safe_int(get("sequence")) or 9999

            # Detect composite key from key_type
            key_type_raw = get("key_type") or ""
            is_pk = _is_truthy(get("primary_key")) or "PK" in key_type_raw.upper()
            is_fk = _is_truthy(get("foreign_key")) or "FK" in key_type_raw.upper()
            is_unique = _is_truthy(get("unique")) or "UK" in key_type_raw.upper() or "UNIQUE" in key_type_raw.upper()
            is_composite = _is_truthy(get("composite_key")) or "CK" in key_type_raw.upper() or "COMPOSITE" in key_type_raw.upper()

            # Foreign key references
            ref_table, ref_column = None, None
            ref_table_raw = get("ref_table")
            ref_column_raw = get("ref_column")
            if ref_table_raw and not ref_column_raw:
                ref_table, ref_column = _parse_ref(ref_table_raw)
            else:
                ref_table = ref_table_raw
                ref_column = ref_column_raw

            # Nullable — Mandatory means NOT nullable
            nullable_raw = get("nullable")
            if nullable_raw:
                s = str(nullable_raw).strip().upper()
                if s in ("MANDATORY","REQUIRED","NOT NULL","N","NO","FALSE","0","NN"):
                    nullable = False
                else:
                    nullable = True
            else:
                nullable = not is_pk  # PKs are NOT NULL by convention

            col = ColumnDef(
                name=col_name,
                data_type=get("data_type") or "VARCHAR",
                length=_safe_int(get("length")),
                precision=_safe_int(get("precision")),
                scale=_safe_int(get("scale")),
                nullable=nullable,
                is_primary_key=is_pk,
                is_foreign_key=is_fk,
                ref_table=ref_table,
                ref_column=ref_column,
                is_unique=is_unique,
                default_value=_safe_any(get("default_value")),
                description=get("description"),
                allowed_values=_parse_allowed(get("allowed_values")),
                min_value=_safe_any(get("min_value")),
                max_value=_safe_any(get("max_value")),
                is_composite_key=is_composite,
                composite_key_group=get("composite_key_group"),
            )

            parsed_rows.append((tbl_key, seq, col))

        # Sort each table's columns by sequence order, then append
        parsed_rows.sort(key=lambda x: (x[0], x[1]))
        for tbl_key, _seq, col in parsed_rows:
            tables[tbl_key].columns.append(col)

        return list(tables.values())
