#!/usr/bin/env python3
"""
Mock Data Generator — CLI Entry Point

Usage:
    python main.py --schema schema.xlsx --rows 1000
    python main.py --schema schema.xlsx --rows 500 --output ./output
    python main.py --schema schema.xlsx --rows CUSTOMERS:500,ORDERS:2000
    python main.py --schema schema.xlsx --rows 1000 --seed 42 --null-pct 0.03
    python main.py --schema schema.xlsx --rows 1000 --validate
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

from schema_parser import SchemaParser, TableDef
from engine import MockDataEngine


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(rows: List[Dict], path: str):
    if not rows:
        Path(path).touch()
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()),
                                quoting=csv.QUOTE_NONNUMERIC,
                                extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------

def validate_output(tables: Dict[str, List[Dict]], schema: Dict[str, TableDef]) -> Dict:
    report = {"tables": {}, "summary": {"total_issues": 0}}

    for tname, rows in tables.items():
        tdef = schema.get(tname.lower())
        issues = []

        if not tdef:
            continue

        # Build column index
        col_map = {c.name.lower(): c for c in tdef.columns}

        # Check primary key uniqueness
        for pk_col in tdef.primary_keys:
            pk_vals = [r.get(pk_col.name) for r in rows]
            duplicates = len(pk_vals) - len(set(pk_vals))
            if duplicates > 0:
                issues.append(f"PK '{pk_col.name}': {duplicates} duplicate(s)")

        # Check NOT NULL constraints
        for col in tdef.columns:
            if not col.nullable and not col.is_primary_key:
                nulls = sum(1 for r in rows if r.get(col.name) is None)
                if nulls > 0:
                    issues.append(f"NOT NULL '{col.name}': {nulls} NULL(s) found")

        # Check FK referential integrity
        fk_pools: Dict[str, set] = {}
        for tbl_name2, rows2 in tables.items():
            t2 = schema.get(tbl_name2.lower())
            if t2:
                for pk_col in t2.primary_keys:
                    key = f"{tbl_name2.lower()}.{pk_col.name.lower()}"
                    fk_pools[key] = {r.get(pk_col.name) for r in rows2}

        for fk_col in tdef.foreign_keys:
            if not fk_col.ref_table or not fk_col.ref_column:
                continue
            pool_key = f"{fk_col.ref_table.lower()}.{fk_col.ref_column.lower()}"
            pool = fk_pools.get(pool_key)
            if pool is None:
                continue
            orphans = sum(
                1 for r in rows
                if r.get(fk_col.name) is not None          # NULL FK is valid (optional ref)
                and str(r.get(fk_col.name)) not in {str(p) for p in pool}
            )
            if orphans > 0:
                issues.append(f"FK '{fk_col.name}' → {fk_col.ref_table}.{fk_col.ref_column}: {orphans} orphan(s)")

        report["tables"][tname] = {
            "row_count": len(rows),
            "issues": issues,
            "status": "PASS" if not issues else "FAIL"
        }
        report["summary"]["total_issues"] += len(issues)

    report["summary"]["status"] = "PASS" if report["summary"]["total_issues"] == 0 else "FAIL"
    return report


# ---------------------------------------------------------------------------
# Parse row counts argument
# ---------------------------------------------------------------------------

def parse_row_counts(raw: str, table_names: List[str]) -> Dict[str, int]:
    """
    Accepts:
      - "500"           → all tables get 500
      - "CUSTOMERS:500,ORDERS:2000"  → per-table
    """
    if ":" in raw:
        result = {}
        for part in raw.split(","):
            part = part.strip()
            if ":" in part:
                tbl, cnt = part.split(":", 1)
                result[tbl.strip().lower()] = int(cnt.strip())
        # Fill missing tables with 100
        for t in table_names:
            if t.lower() not in result:
                result[t.lower()] = 100
        return result
    else:
        n = int(raw)
        return {t.lower(): n for t in table_names}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate production-quality mock data from a Collibra Excel schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--schema", required=True,
                        help="Path to the Collibra Excel schema file (.xlsx)")
    parser.add_argument("--rows", default="100",
                        help="Number of rows per table, or TABLE:N,TABLE2:N2 format")
    parser.add_argument("--output", default="./output",
                        help="Output directory for CSV files (default: ./output)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducibility")
    parser.add_argument("--null-pct", type=float, default=0.05,
                        help="Probability of NULL for nullable columns (default: 0.05)")
    parser.add_argument("--validate", action="store_true",
                        help="Run validation after generation and print report")
    parser.add_argument("--format", choices=["csv","json","both"], default="csv",
                        help="Output format (default: csv)")
    parser.add_argument("--list-tables", action="store_true",
                        help="List tables found in schema and exit")

    args = parser.parse_args()

    # ---- Parse schema ----
    print(f"\n{'='*60}")
    print("  Mock Data Generator — Collibra Schema")
    print(f"{'='*60}")
    print(f"\n[1/4] Parsing schema: {args.schema}")

    try:
        sp = SchemaParser()
        tables = sp.parse_excel(args.schema)
    except Exception as e:
        print(f"\n✗ Schema parse error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  ✓ Found {len(tables)} table(s):")
    for t in tables:
        pks = [c.name for c in t.primary_keys]
        fks = [f"{c.name}→{c.ref_table}" for c in t.foreign_keys]
        print(f"    • {t.schema}.{t.name} ({len(t.columns)} cols | PKs: {pks} | FKs: {fks})")

    if args.list_tables:
        sys.exit(0)

    # ---- Row counts ----
    table_names = [t.name for t in tables]
    try:
        row_counts = parse_row_counts(args.rows, table_names)
    except ValueError as e:
        print(f"\n✗ Invalid --rows argument: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n[2/4] Generating data:")
    for t in tables:
        n = row_counts.get(t.name.lower(), 100)
        print(f"    • {t.name}: {n:,} rows")

    start = time.time()

    # ---- Generate ----
    engine = MockDataEngine(
        tables=tables,
        row_counts=row_counts,
        null_probability=args.null_pct,
        seed=args.seed,
    )

    try:
        all_data = engine.generate_all()
    except Exception as e:
        print(f"\n✗ Generation error: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)

    elapsed = time.time() - start
    total_rows = sum(len(v) for v in all_data.values())
    print(f"\n  ✓ Generated {total_rows:,} total rows in {elapsed:.2f}s")

    # ---- Write output ----
    os.makedirs(args.output, exist_ok=True)
    print(f"\n[3/4] Writing output to: {args.output}/")

    for tname, rows in all_data.items():
        if args.format in ("csv", "both"):
            out_path = os.path.join(args.output, f"{tname}.csv")
            write_csv(rows, out_path)
            sz = os.path.getsize(out_path)
            print(f"    • {tname}.csv  ({len(rows):,} rows, {sz/1024:.1f} KB)")

        if args.format in ("json", "both"):
            out_path = os.path.join(args.output, f"{tname}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, default=str)
            print(f"    • {tname}.json  ({len(rows):,} rows)")

    # ---- Validation ----
    if args.validate:
        print(f"\n[4/4] Validating constraints...")
        schema_map = {t.name.lower(): t for t in tables}
        report = validate_output(all_data, schema_map)

        for tname, tinfo in report["tables"].items():
            status_icon = "✓" if tinfo["status"] == "PASS" else "✗"
            print(f"    {status_icon} {tname}: {tinfo['row_count']:,} rows — {tinfo['status']}")
            for issue in tinfo["issues"]:
                print(f"      ⚠ {issue}")

        # Write validation report
        report_path = os.path.join(args.output, "_validation_report.json")
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n  Validation report: {report_path}")
        print(f"  Overall: {report['summary']['status']} "
              f"({report['summary']['total_issues']} issue(s))")
    else:
        print(f"\n[4/4] Skipping validation (use --validate to enable)")

    print(f"\n{'='*60}")
    print(f"  Done! Output in: {os.path.abspath(args.output)}/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
