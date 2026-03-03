"""
Mock Data Engine — orchestrates data generation with:
  - Primary key uniqueness (single and composite)
  - Foreign key referential integrity
  - Unique constraint enforcement
  - Semantic-aware value generation
  - NULL distribution based on schema definitions
  - Production-quality realistic data
"""

import random
import uuid
import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

from schema_parser import ColumnDef, TableDef
from generator import (
    _detect_semantic, _gen_semantic, _gen_by_dtype, _weighted_none,
    _rand_date, _rand_datetime
)


# ---------------------------------------------------------------------------
# Key registry — tracks generated keys for FK / uniqueness enforcement
# ---------------------------------------------------------------------------

class KeyRegistry:
    def __init__(self):
        # table_name -> column_name -> set of generated values
        self._pk_store: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        # table_name -> set of tuple(values) for composite PKs
        self._composite_pk_store: Dict[str, Set[tuple]] = defaultdict(set)
        # table_name -> column_name -> set of generated values (unique cols)
        self._unique_store: Dict[str, Dict[str, Set]] = defaultdict(lambda: defaultdict(set))

    def register_pk(self, table: str, col: str, value: Any):
        self._pk_store[table][col].append(value)

    def register_composite_pk(self, table: str, key_tuple: tuple):
        self._composite_pk_store[table].add(key_tuple)

    def composite_pk_exists(self, table: str, key_tuple: tuple) -> bool:
        return key_tuple in self._composite_pk_store[table]

    def get_pk_values(self, table: str, col: str) -> list:
        return self._pk_store[table][col]

    def register_unique(self, table: str, col: str, value: Any):
        self._unique_store[table][col].add(value)

    def unique_exists(self, table: str, col: str, value: Any) -> bool:
        return value in self._unique_store[table][col]

    def has_pk_values(self, table: str, col: str) -> bool:
        return bool(self._pk_store[table][col])


# ---------------------------------------------------------------------------
# Primary key generators
# ---------------------------------------------------------------------------

def _gen_pk_value(col: ColumnDef, row_idx: int, counter: int) -> Any:
    """Generate a guaranteed-unique primary key value."""
    dt = (col.data_type or "").upper()

    if "UUID" in dt or "GUID" in dt or "UNIQUEIDENTIFIER" in dt:
        return str(uuid.uuid4())

    if any(x in dt for x in ["INT","SERIAL","BIGINT","NUMBER","NUMERIC","SMALLINT"]):
        return counter + 1

    if any(x in dt for x in ["CHAR","VARCHAR","TEXT","STRING"]):
        name_prefix = col.name.upper()[:3].replace(" ", "")
        return f"{name_prefix}{counter + 1:08d}"

    if "UUID" in col.name.upper() or "GUID" in col.name.upper():
        return str(uuid.uuid4())

    return counter + 1


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

class MockDataEngine:

    def __init__(self, tables: List[TableDef], row_counts: Dict[str, int],
                 null_probability: float = 0.05, seed: Optional[int] = None):
        self.tables = {t.name.lower(): t for t in tables}
        self.row_counts = {k.lower(): v for k, v in row_counts.items()}
        self.null_prob = null_probability
        self.registry = KeyRegistry()
        if seed is not None:
            random.seed(seed)

        # Build topological order
        self._order = self._topological_sort()

    # ------------------------------------------------------------------
    # Topological sort for FK dependency resolution
    # ------------------------------------------------------------------

    def _topological_sort(self) -> List[str]:
        """Sort tables so parent tables are generated before child tables."""
        deps: Dict[str, Set[str]] = {}
        for tname, tdef in self.tables.items():
            dep_set = set()
            for col in tdef.foreign_keys:
                if col.ref_table:
                    ref = col.ref_table.lower()
                    if ref in self.tables and ref != tname:
                        dep_set.add(ref)
            deps[tname] = dep_set

        ordered = []
        visited = set()
        in_progress = set()

        def visit(n):
            if n in visited:
                return
            if n in in_progress:
                # Circular FK — skip (will use None for unresolvable FK)
                return
            in_progress.add(n)
            for dep in deps.get(n, set()):
                visit(dep)
            in_progress.discard(n)
            visited.add(n)
            ordered.append(n)

        for tname in self.tables:
            visit(tname)

        return ordered

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_all(self) -> Dict[str, List[Dict]]:
        """Generate mock data for all tables. Returns dict of table_name -> rows."""
        results = {}
        for tname in self._order:
            if tname not in self.tables:
                continue
            n = self.row_counts.get(tname, 100)
            tdef = self.tables[tname]
            rows = self._generate_table(tdef, n)
            results[tname] = rows
        return results

    # ------------------------------------------------------------------
    # Table generation
    # ------------------------------------------------------------------

    def _generate_table(self, tdef: TableDef, n: int) -> List[Dict]:
        rows = []
        # Per-column counters for PK sequences
        pk_counters: Dict[str, int] = defaultdict(int)
        # Track unique values per column within this table
        unique_seen: Dict[str, Set] = defaultdict(set)
        # Track composite PK tuples
        composite_pk_seen: Set[tuple] = set()

        composite_groups = tdef.composite_key_groups
        composite_col_names = {
            col.name
            for cols in composite_groups.values()
            for col in cols
        }

        for i in range(n):
            row = {}

            # ---- Pass 1: Generate all non-composite-PK columns ----
            for col in tdef.columns:
                if col.name in composite_col_names:
                    continue  # handled separately

                value = self._gen_column_value(
                    col, tdef.name, i, pk_counters, unique_seen
                )
                row[col.name] = value

            # ---- Pass 2: Composite key columns ----
            for group_name, group_cols in composite_groups.items():
                max_attempts = 100
                for attempt in range(max_attempts):
                    combo = {}
                    for col in group_cols:
                        combo[col.name] = self._gen_column_value(
                            col, tdef.name, i, pk_counters, unique_seen
                        )
                    key_tuple = tuple(combo[c.name] for c in group_cols)
                    if key_tuple not in composite_pk_seen:
                        composite_pk_seen.add(key_tuple)
                        self.registry.register_composite_pk(tdef.name.lower(), key_tuple)
                        row.update(combo)
                        break
                else:
                    # Fallback: append row index to first column
                    for col in group_cols:
                        row[col.name] = f"{i}_{random.randint(0,99999)}"

            rows.append(row)

            # Register PKs for FK use
            for col in tdef.primary_keys:
                if col.name in row and row[col.name] is not None:
                    self.registry.register_pk(tdef.name.lower(), col.name.lower(), row[col.name])

        return rows

    # ------------------------------------------------------------------
    # Column value generation
    # ------------------------------------------------------------------

    def _gen_column_value(
        self,
        col: ColumnDef,
        table_name: str,
        row_idx: int,
        pk_counters: Dict[str, int],
        unique_seen: Dict[str, Set],
    ) -> Any:

        # ---- Primary Key ----
        if col.is_primary_key:
            pk_counters[col.name] += 1
            value = _gen_pk_value(col, row_idx, pk_counters[col.name] - 1)
            self.registry.register_unique(table_name.lower(), col.name.lower(), value)
            return value

        # ---- Foreign Key ----
        if col.is_foreign_key and col.ref_table:
            value = self._resolve_fk(col, row_idx)
            if value is not None:
                return value
            # FK pool empty (e.g. self-referencing on first row): return NULL if nullable
            if col.nullable:
                return None
            # Not nullable and no pool yet — will be NULL, but can't do better

        # ---- Nullable with NULL injection ----
        null_pct = self.null_prob if col.nullable else 0.0
        # Columns with "NOT NULL" semantics
        if not col.nullable:
            null_pct = 0.0

        # ---- Default value ----
        if col.default_value is not None and random.random() < 0.1:
            return col.default_value

        # ---- Allowed values (enum) ----
        if col.allowed_values:
            value = random.choice(col.allowed_values)
            if null_pct and random.random() < null_pct:
                return None
            return value

        # ---- Semantic detection ----
        semantic = _detect_semantic(col.name, col.data_type)
        if semantic:
            value = _gen_semantic(semantic, row_idx, col.name)
            if value is None:
                value = _gen_by_dtype(
                    col.name, col.data_type, col.length,
                    col.precision, col.scale,
                    col.min_value, col.max_value, col.allowed_values
                )
        else:
            value = _gen_by_dtype(
                col.name, col.data_type, col.length,
                col.precision, col.scale,
                col.min_value, col.max_value, col.allowed_values
            )

        # ---- Unique constraint enforcement ----
        if (col.is_unique or col.is_primary_key) and value is not None:
            max_attempts = 1000
            for _ in range(max_attempts):
                if not self.registry.unique_exists(table_name.lower(), col.name.lower(), value):
                    break
                # Regenerate
                if semantic:
                    value = _gen_semantic(semantic, row_idx + random.randint(1, 9999), col.name)
                else:
                    value = _gen_by_dtype(
                        col.name, col.data_type, col.length,
                        col.precision, col.scale,
                        col.min_value, col.max_value, col.allowed_values
                    )
            self.registry.register_unique(table_name.lower(), col.name.lower(), value)

        # ---- Apply NULL probability ----
        if null_pct and random.random() < null_pct:
            return None

        return value

    # ------------------------------------------------------------------
    # Foreign key resolution
    # ------------------------------------------------------------------

    def _resolve_fk(self, col: ColumnDef, row_idx: int) -> Any:
        """Return a value from the referenced table's PK pool."""
        ref_tbl = col.ref_table.lower() if col.ref_table else None
        ref_col = col.ref_column.lower() if col.ref_column else None

        if not ref_tbl:
            return None

        # Try exact ref_col first
        if ref_col and self.registry.has_pk_values(ref_tbl, ref_col):
            pool = self.registry.get_pk_values(ref_tbl, ref_col)
            return random.choice(pool)

        # Try to find any PK of the referenced table
        if ref_tbl in self.tables:
            ref_tdef = self.tables[ref_tbl]
            for pk_col in ref_tdef.primary_keys:
                if self.registry.has_pk_values(ref_tbl, pk_col.name.lower()):
                    pool = self.registry.get_pk_values(ref_tbl, pk_col.name.lower())
                    return random.choice(pool)

        # No data yet generated for referenced table (e.g. self-referencing FK on first row)
        return None
