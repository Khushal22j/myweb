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


def write_csv(rows, path):
    if not rows:
        Path(path).touch()
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()),
                                quoting=csv.QUOTE_NONNUMERIC, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def validate_output(tables, schema):
    report = {"tables": {}, "summary": {"total_issues": 0}}
    for tname, rows in tables.items():
        tdef = schema.get(tname.lower())
        issues = []
        if not tdef:
            continue
        for pk_col in tdef.primary_keys:
            pk_vals = [str(r.get(pk_col.name)) for r in rows]
            dups = len(pk_vals) - len(set(pk_vals))
            if dups > 0:
                issues.append(f"PK '{pk_col.name}': {dups} duplicate(s)")
        for col in tdef.columns:
            if not col.nullable and not col.is_primary_key:
                nulls = sum(1 for r in rows if r.get(col.name) is None or r.get(col.name) == "")
                if nulls > 0:
                    issues.append(f"NOT NULL '{col.name}': {nulls} NULL(s)")
        fk_pools = {}
        for tbl2, rows2 in tables.items():
            t2 = schema.get(tbl2.lower())
            if t2:
                for pk in t2.primary_keys:
                    fk_pools[f"{tbl2.lower()}.{pk.name.lower()}"] = {str(r.get(pk.name)) for r in rows2}
        for fk in tdef.foreign_keys:
            if not fk.ref_table or not fk.ref_column:
                continue
            pool = fk_pools.get(f"{fk.ref_table.lower()}.{fk.ref_column.lower()}")
            if pool is None:
                continue
            orphans = sum(1 for r in rows
                          if r.get(fk.name) is not None and r.get(fk.name) != ""
                          and str(r.get(fk.name)) not in pool)
            if orphans > 0:
                issues.append(f"FK '{fk.name}' -> {fk.ref_table}.{fk.ref_column}: {orphans} orphan(s)")
        report["tables"][tname] = {"row_count": len(rows), "issues": issues,
                                    "status": "PASS" if not issues else "FAIL"}
        report["summary"]["total_issues"] += len(issues)
    report["summary"]["status"] = "PASS" if report["summary"]["total_issues"] == 0 else "FAIL"
    return report


def parse_row_counts(raw, table_names):
    if ":" in raw:
        result = {}
        for part in raw.split(","):
            if ":" in part:
                tbl, cnt = part.strip().split(":", 1)
                result[tbl.strip().lower()] = int(cnt.strip())
        for t in table_names:
            if t.lower() not in result:
                result[t.lower()] = 100
        return result
    return {t.lower(): int(raw) for t in table_names}


def main():
    parser = argparse.ArgumentParser(description="Generate mock data from Excel schema.")
    parser.add_argument("--schema", required=True, help="Path to Excel schema file (.xlsx)")
    parser.add_argument("--rows", default="100", help="Rows per table, e.g. 1000 or TABLE1:500,TABLE2:2000")
    parser.add_argument("--output", default="./output", help="Output folder (default: ./output)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--null-pct", type=float, default=0.05, help="NULL % for nullable columns (default: 0.05)")
    parser.add_argument("--validate", action="store_true", help="Validate constraints after generation")
    parser.add_argument("--format", choices=["csv", "json", "both"], default="csv")
    parser.add_argument("--list-tables", action="store_true", help="List tables in schema and exit")
    args = parser.parse_args()

    print(f"\n{'='*60}\n  Mock Data Generator\n{'='*60}")
    print(f"\n[1/4] Reading schema: {args.schema}")

    try:
        tables = SchemaParser().parse_excel(args.schema)
    except Exception as e:
        print(f"\n  ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Found {len(tables)} table(s):")
    for t in tables:
        pks = [c.name for c in t.primary_keys]
        fks = [f"{c.name}->{c.ref_table}" for c in t.foreign_keys]
        info = []
        if pks: info.append(f"PK: {pks}")
        if fks: info.append(f"FK: {fks}")
        print(f"    * {t.name}  ({len(t.columns)} columns{(', ' + ', '.join(info)) if info else ''})")

    if args.list_tables:
        sys.exit(0)

    row_counts = parse_row_counts(args.rows, [t.name for t in tables])

    print(f"\n[2/4] Generating data:")
    for t in tables:
        print(f"    * {t.name}: {row_counts.get(t.name.lower(), 100):,} rows")

    start = time.time()
    engine = MockDataEngine(tables=tables, row_counts=row_counts,
                             null_probability=args.null_pct, seed=args.seed)
    try:
        all_data = engine.generate_all()
    except Exception as e:
        print(f"\n  ERROR: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)

    elapsed = time.time() - start
    print(f"\n  Done — {sum(len(v) for v in all_data.values()):,} total rows in {elapsed:.2f}s")

    os.makedirs(args.output, exist_ok=True)
    print(f"\n[3/4] Writing files to: {args.output}/")

    for tname, rows in all_data.items():
        if args.format in ("csv", "both"):
            out_path = os.path.join(args.output, f"{tname}.csv")
            write_csv(rows, out_path)
            print(f"    * {tname}.csv  ({len(rows):,} rows, {os.path.getsize(out_path)/1024:.1f} KB)")
        if args.format in ("json", "both"):
            out_path = os.path.join(args.output, f"{tname}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, indent=2, default=str)
            print(f"    * {tname}.json  ({len(rows):,} rows)")

    if args.validate:
        print(f"\n[4/4] Validating constraints...")
        schema_map = {t.name.lower(): t for t in tables}
        report = validate_output(all_data, schema_map)
        for tname, info in report["tables"].items():
            icon = "PASS" if info["status"] == "PASS" else "FAIL"
            print(f"    [{icon}] {tname}: {info['row_count']:,} rows")
            for issue in info["issues"]:
                print(f"        ! {issue}")
        rpath = os.path.join(args.output, "_validation_report.json")
        with open(rpath, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\n  Overall: {report['summary']['status']} ({report['summary']['total_issues']} issue(s))")
        print(f"  Report saved: {rpath}")
    else:
        print(f"\n[4/4] Tip: add --validate to check PK/FK/NULL constraints")

    print(f"\n{'='*60}\n  Output: {os.path.abspath(args.output)}\n{'='*60}\n")


if __name__ == "__main__":
    main()