"""
Mock Data Generator
====================
Reads a Collibra-style Excel schema file and generates realistic mock data
using the Faker library.

Excel format (each sheet = one table):
    Headers: Name | Sequence Order | Data Type | Length | Scale | Is Primary Key?

Usage:
    pip install faker openpyxl pandas

    python mock_data_generator.py --schema yourfile.xlsx --rows 1000
    python mock_data_generator.py --schema yourfile.xlsx --rows 1000 --output ./output
    python mock_data_generator.py --schema yourfile.xlsx --rows "SHEET1:500,SHEET2:2000"
    python mock_data_generator.py --schema yourfile.xlsx --rows 1000 --seed 42
"""

import argparse
import csv
import os
import re
import sys
import uuid
import random
import hashlib
import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from faker import Faker

fake = Faker()


# ─────────────────────────────────────────────────────────────────────────────
# Schema Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColumnDef:
    name: str
    data_type: str = "VARCHAR"
    length: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    ref_table: Optional[str] = None
    ref_column: Optional[str] = None
    is_unique: bool = False
    allowed_values: Optional[List[str]] = None
    min_value: Any = None
    max_value: Any = None
    is_composite_key: bool = False
    composite_key_group: Optional[str] = None


@dataclass
class TableDef:
    name: str
    columns: List[ColumnDef] = field(default_factory=list)

    @property
    def primary_keys(self):
        return [c for c in self.columns if c.is_primary_key]

    @property
    def foreign_keys(self):
        return [c for c in self.columns if c.is_foreign_key]

    @property
    def composite_key_groups(self):
        groups = {}
        for col in self.columns:
            if col.is_composite_key and col.composite_key_group:
                groups.setdefault(col.composite_key_group, []).append(col)
        return groups


# ─────────────────────────────────────────────────────────────────────────────
# Header Mapping  (covers your exact headers + common synonyms)
# ─────────────────────────────────────────────────────────────────────────────

HEADER_MAP = {
    # Column name  ← your exact header: "Name"
    "name":                     "column",
    "column":                   "column",
    "column name":              "column",
    "attribute":                "column",
    "attribute name":           "column",
    "field":                    "column",
    "field name":               "column",
    "element":                  "column",

    # Sequence  ← your exact header: "Sequence Order"
    "sequence order":           "sequence",
    "sequence_order":           "sequence",
    "sequence":                 "sequence",
    "ordinal position":         "sequence",
    "position":                 "sequence",
    "column order":             "sequence",

    # Data type  ← your exact header: "Data Type"
    "data type":                "data_type",
    "data_type":                "data_type",
    "datatype":                 "data_type",
    "type":                     "data_type",
    "technical data type":      "data_type",
    "attribute type":           "data_type",
    "physical type":            "data_type",

    # Length  ← your exact header: "Length"
    "length":                   "length",
    "max length":               "length",
    "size":                     "length",
    "char length":              "length",
    "character maximum length": "length",

    # Scale  ← your exact header: "Scale"
    "scale":                    "scale",
    "numeric scale":            "scale",
    "decimal places":           "scale",

    # Nullable
    "nullable":                 "nullable",
    "is nullable":              "nullable",
    "nullability":              "nullable",
    "mandatory":                "nullable",
    "required":                 "nullable",
    "not null":                 "nullable",
    "is required":              "nullable",
    "is mandatory":             "nullable",

    # Primary key  ← your exact header: "Is Primary Key?"
    "is primary key?":          "primary_key",
    "is primary key":           "primary_key",
    "primary key":              "primary_key",
    "primary key?":             "primary_key",
    "pk":                       "primary_key",
    "is pk":                    "primary_key",
    "is pk?":                   "primary_key",
    "pk?":                      "primary_key",

    # Foreign key
    "is foreign key?":          "foreign_key",
    "is foreign key":           "foreign_key",
    "foreign key":              "foreign_key",
    "fk":                       "foreign_key",
    "is fk?":                   "foreign_key",

    # FK references
    "reference table":          "ref_table",
    "ref table":                "ref_table",
    "referenced table":         "ref_table",
    "fk table":                 "ref_table",
    "parent table":             "ref_table",
    "references":               "ref_table",
    "reference column":         "ref_column",
    "ref column":               "ref_column",
    "referenced column":        "ref_column",
    "fk column":                "ref_column",
    "parent column":            "ref_column",

    # Unique
    "unique":                   "unique",
    "is unique":                "unique",
    "is unique?":               "unique",
    "unique constraint":        "unique",

    # Allowed values
    "allowed values":           "allowed_values",
    "valid values":             "allowed_values",
    "enum":                     "allowed_values",
    "enumeration":              "allowed_values",
    "domain values":            "allowed_values",
    "code list":                "allowed_values",
    "permissible values":       "allowed_values",
    "value list":               "allowed_values",

    # Min / Max
    "min":                      "min_value",
    "minimum":                  "min_value",
    "min value":                "min_value",
    "max":                      "max_value",
    "maximum":                  "max_value",
    "max value":                "max_value",

    # Composite key
    "composite key":            "composite_key",
    "is composite key":         "composite_key",
    "is composite key?":        "composite_key",
    "composite key group":      "composite_key_group",
    "key group":                "composite_key_group",
    "constraint name":          "composite_key_group",
}


def _map_header(h: str) -> Optional[str]:
    return HEADER_MAP.get(str(h).lower().strip())


def _is_truthy(val) -> bool:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return False
    return str(val).strip().upper() in (
        "YES", "Y", "TRUE", "1", "X", "PK", "FK", "UNIQUE", "PRIMARY",
        "FOREIGN", "MANDATORY", "NOT NULL", "NN"
    )


def _safe_int(val) -> Optional[int]:
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(float(str(val).strip()))
    except Exception:
        return None


def _safe_str(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none", "null", "") else None


def _parse_allowed(val) -> Optional[List[str]]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    for sep in ["|", ";", ","]:
        if sep in s:
            return [v.strip() for v in s.split(sep) if v.strip()]
    return [s]


def _parse_ref(ref_str: str):
    if not ref_str:
        return None, None
    m = re.match(r"^([^.(]+)[.(]([^.)]+)[)]?$", ref_str.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    if "." in ref_str:
        parts = ref_str.split(".", 1)
        return parts[0].strip(), parts[1].strip()
    return ref_str.strip(), None


# ─────────────────────────────────────────────────────────────────────────────
# Schema Parser  (each sheet tab = one table)
# ─────────────────────────────────────────────────────────────────────────────

def parse_excel(path: str) -> List[TableDef]:
    xl = pd.read_excel(path, sheet_name=None, header=None, dtype=str)
    all_tables: Dict[str, TableDef] = {}

    for sheet_name, raw_df in xl.items():
        if raw_df.empty:
            continue

        # Find header row (first row with ≥2 recognized headers)
        header_row_idx = None
        for i in range(min(10, len(raw_df))):
            row = raw_df.iloc[i]
            matches = sum(1 for cell in row if _map_header(str(cell or "")) is not None)
            if matches >= 2:
                header_row_idx = i
                break
        if header_row_idx is None:
            continue

        headers = [str(h) if h is not None else "" for h in raw_df.iloc[header_row_idx]]

        # Build field -> column index map
        mapped: Dict[str, int] = {}
        for idx, h in enumerate(headers):
            field = _map_header(h)
            if field is not None:
                # "name" alone → "column", but don't override a more specific header
                if h.strip().lower() == "name" and "column" in mapped:
                    continue
                mapped[field] = idx

        if "column" not in mapped:
            continue

        data = raw_df.iloc[header_row_idx + 1:].reset_index(drop=True)
        data = data.dropna(how="all")

        # Sheet name = table name
        table_name = sheet_name.strip()
        if table_name not in all_tables:
            all_tables[table_name] = TableDef(name=table_name)

        parsed_rows = []

        for _, row in data.iterrows():
            def get(f, _row=row):
                idx = mapped.get(f)
                if idx is None or idx >= len(_row):
                    return None
                val = _row.iloc[idx]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                s = str(val).strip()
                return s if s and s.lower() not in ("nan", "none", "null", "") else None

            col_name = get("column")
            if not col_name:
                continue

            seq = _safe_int(get("sequence")) or 9999

            is_pk        = _is_truthy(get("primary_key"))
            is_fk        = _is_truthy(get("foreign_key"))
            is_unique    = _is_truthy(get("unique"))
            is_composite = _is_truthy(get("composite_key"))

            ref_table, ref_column = None, None
            ref_table_raw  = get("ref_table")
            ref_column_raw = get("ref_column")
            if ref_table_raw and not ref_column_raw:
                ref_table, ref_column = _parse_ref(ref_table_raw)
            else:
                ref_table  = ref_table_raw
                ref_column = ref_column_raw

            nullable_raw = get("nullable")
            if nullable_raw:
                nullable = str(nullable_raw).upper() not in (
                    "MANDATORY", "REQUIRED", "NOT NULL", "N", "NO", "FALSE", "0", "NN"
                )
            else:
                nullable = not is_pk

            col = ColumnDef(
                name=col_name,
                data_type=get("data_type") or "VARCHAR",
                length=_safe_int(get("length")),
                scale=_safe_int(get("scale")),
                nullable=nullable,
                is_primary_key=is_pk,
                is_foreign_key=is_fk,
                ref_table=ref_table,
                ref_column=ref_column,
                is_unique=is_unique,
                allowed_values=_parse_allowed(get("allowed_values")),
                min_value=_safe_str(get("min_value")),
                max_value=_safe_str(get("max_value")),
                is_composite_key=is_composite,
                composite_key_group=get("composite_key_group"),
            )
            parsed_rows.append((seq, col))

        # Sort by sequence order
        parsed_rows.sort(key=lambda x: x[0])
        for _, col in parsed_rows:
            all_tables[table_name].columns.append(col)

    if not all_tables:
        raise ValueError(
            "No tables found in the Excel file.\n"
            "Each sheet tab = one table.\n"
            "Required headers: Name, Data Type\n"
            "Optional: Sequence Order, Length, Scale, Is Primary Key?"
        )

    return list(all_tables.values())


# ─────────────────────────────────────────────────────────────────────────────
# Faker-based Value Generator
# ─────────────────────────────────────────────────────────────────────────────

# Semantic pattern → generator tag
SEMANTIC_PATTERNS = [
    # IDs
    (r"\b(uuid|guid)\b",                                        "uuid"),
    (r"\b(id|identifier|key)\b$",                               "generic_id"),

    # Person
    (r"\b(first.?name|given.?name|fname)\b",                    "first_name"),
    (r"\b(last.?name|surname|family.?name|lname)\b",            "last_name"),
    (r"\bfull.?name\b",                                          "full_name"),
    (r"\b(middle.?name|middle.?initial)\b",                     "middle_name"),
    (r"\bprefix\b",                                              "name_prefix"),
    (r"\bsuffix\b",                                              "name_suffix"),
    (r"\bgender\b",                                              "gender"),
    (r"\bage\b",                                                  "age"),
    (r"\b(dob|date.?of.?birth|birth.?date)\b",                  "dob"),
    (r"\bnationality\b",                                         "nationality"),

    # Contact
    (r"\bemail\b",                                               "email"),
    (r"\b(phone|mobile|cell|fax|telephone|contact.?no)\b",      "phone"),
    (r"\b(street|street.?address)\b",                           "street"),
    (r"\b(address|addr)\b",                                      "address"),
    (r"\b(city|town|municipality)\b",                           "city"),
    (r"\bstate.?code\b",                                         "state_code"),
    (r"\b(state|province|region)\b",                            "state"),
    (r"\b(zip|postal|post.?code|zipcode)\b",                    "zipcode"),
    (r"\bcountry.?code\b",                                       "country_code"),
    (r"\b(country|nation)\b",                                   "country"),
    (r"\blatitude\b",                                            "latitude"),
    (r"\blongitude\b",                                           "longitude"),
    (r"\bwebsite\b",                                             "url"),

    # Organisation
    (r"\b(company|organization|org|firm|employer|company.?name)\b", "company"),
    (r"\b(department|dept)\b",                                  "department"),
    (r"\bbusiness.?unit\b",                                     "department"),
    (r"\b(job.?title|position|role|designation|occupation)\b",  "job_title"),
    (r"\bemployee.?type\b",                                     "employee_type"),
    (r"\bindustry\b",                                            "industry"),

    # Finance
    (r"\b(unit.?price|list.?price|sale.?price|sell.?price)\b",  "money"),
    (r"\b(price|amount|cost|fee|charge|salary|wage|revenue|income|balance|total|subtotal|tax|discount|budget|spend|earning|payment)\b", "money"),
    (r"\b(currency|currency.?code|ccy)\b",                      "currency"),
    (r"\b(account.?number|acct.?num|account.?no|acct.?number)\b", "account_number"),
    (r"\b(credit.?card|card.?number|pan)\b",                    "credit_card"),
    (r"\b(card.?type|card.?brand)\b",                           "card_type"),
    (r"\b(bank.?name|bank)\b",                                  "bank_name"),
    (r"\b(routing.?number|aba)\b",                              "routing_number"),
    (r"\b(transaction.?id|txn.?id|txn.?number)\b",             "txn_id"),
    (r"\b(invoice.?number|invoice.?id)\b",                      "invoice_number"),
    (r"\b(order.?number|order.?id|order.?no)\b",                "order_number"),
    (r"\b(sku|product.?code|item.?code)\b",                     "sku"),

    # Dates & Times
    (r"\b(created.?at|created.?date|creation.?date|created.?on)\b", "datetime_past"),
    (r"\b(updated.?at|updated.?date|modified.?at|last.?modified)\b", "datetime_recent"),
    (r"\b(deleted.?at|deleted.?date)\b",                        "datetime_nullable"),
    (r"\b(start.?date|begin.?date|from.?date|effective.?date)\b", "date_past"),
    (r"\b(end.?date|expiry.?date|expiration.?date|to.?date|due.?date)\b", "date_future"),
    (r"\b(date|dt)\b",                                           "date_past"),
    (r"\btimestamp\b",                                           "datetime_past"),
    (r"\btime\b",                                                "time"),
    (r"\byear\b",                                                "year"),
    (r"\bmonth\b",                                               "month"),

    # Status / Flags
    (r"\bstatus\b",                                              "status"),
    (r"\b(is.?\w+|has.?\w+|flag|active|enabled|verified|deleted)\b", "boolean"),
    (r"\b(type|category|kind|class|tier|grade)\b",              "category"),
    (r"\bpriority\b",                                            "priority"),
    (r"\bseverity\b",                                            "severity"),
    (r"\blevel\b",                                               "level"),

    # System / IT
    (r"\b(ip.?address|ip.?addr|ipv4)\b",                        "ipv4"),
    (r"\bipv6\b",                                                "ipv6"),
    (r"\bmac.?address\b",                                        "mac_address"),
    (r"\b(url|uri|link|endpoint)\b",                            "url"),
    (r"\b(username|user.?name|login)\b",                        "username"),
    (r"\b(password|passwd|pwd|secret)\b",                       "password_hash"),
    (r"\btoken\b",                                               "token"),
    (r"\bsession.?id\b",                                         "session_id"),
    (r"\bapi.?key\b",                                            "api_key"),
    (r"\blog.?level\b",                                          "log_level"),
    (r"\bhttp.?method\b",                                        "http_method"),
    (r"\b(http.?status|status.?code|response.?code)\b",         "http_status"),
    (r"\bplatform\b",                                            "platform"),
    (r"\bbrowser\b",                                             "browser"),
    (r"\bos\b",                                                   "os"),
    (r"\bversion\b",                                             "version"),
    (r"\bfile.?name\b",                                          "filename"),
    (r"\bfile.?size\b",                                          "file_size"),
    (r"\b(checksum|hash|md5|sha)\b",                            "md5"),
    (r"\buser.?agent\b",                                         "user_agent"),
    (r"\bdevice.?id\b",                                          "device_id"),

    # Text / Content
    (r"\b(description|desc|details|notes?|comments?|remarks?|summary|bio|about|message|body|content|text|narrative)\b", "text"),
    (r"\b(title|heading|headline|subject)\b",                   "sentence"),
    (r"\btag\b",                                                  "tags"),
    (r"\b(color|colour)\b",                                     "color"),
    (r"\b(language|lang|locale)\b",                             "language"),
    (r"\b(timezone|time.?zone|tz)\b",                           "timezone"),

    # Numeric
    (r"\b(quantity|qty|count|num|number)\b",                    "quantity"),
    (r"\b(percentage|percent|pct|ratio)\b",                     "percentage"),
    (r"\b(score|rating|rank)\b",                                "score"),
    (r"\bweight\b",                                              "weight"),
    (r"\bheight\b",                                              "height"),
    (r"\bduration\b",                                            "duration"),
    (r"\b(latitude|lat)\b",                                     "latitude"),
    (r"\b(longitude|lon|lng)\b",                                "longitude"),

    # Product / Inventory
    (r"\bproduct.?name\b",                                       "product_name"),
    (r"\b(product.?category|item.?category)\b",                 "product_category"),
    (r"\bbrand\b",                                               "brand"),
    (r"\bmodel\b",                                               "model"),
    (r"\bserial.?number\b",                                      "serial_number"),
    (r"\bbarcode\b",                                             "barcode"),
    (r"\bstock\b",                                               "stock"),
]


def detect_semantic(col_name: str) -> Optional[str]:
    n = col_name.lower().replace("_", " ").replace("-", " ")
    for pattern, tag in SEMANTIC_PATTERNS:
        if re.search(pattern, n):
            return tag
    return None


def generate_value(tag: str, row_idx: int, col_name: str) -> Any:
    """Generate a realistic value using Faker for the given semantic tag."""
    f = fake

    # ── IDs ──────────────────────────────────────────────────────────────
    if tag == "uuid":
        return str(uuid.uuid4())
    if tag == "generic_id":
        prefix = re.sub(r"[^A-Z]", "", col_name.upper())[:4] or "ID"
        return f"{prefix}{row_idx + 1:08d}"

    # ── Person ────────────────────────────────────────────────────────────
    if tag == "first_name":
        return f.first_name()
    if tag == "last_name":
        return f.last_name()
    if tag == "full_name":
        return f.name()
    if tag == "middle_name":
        return f.first_name()
    if tag == "name_prefix":
        return f.prefix()
    if tag == "name_suffix":
        return f.suffix()
    if tag == "gender":
        return random.choice(["M", "F", "MALE", "FEMALE", "NON_BINARY", "PREFER_NOT_TO_SAY"])
    if tag == "age":
        return random.randint(18, 75)
    if tag == "dob":
        return f.date_of_birth(minimum_age=18, maximum_age=75).isoformat()
    if tag == "nationality":
        return f.country()

    # ── Contact ────────────────────────────────────────────────────────────
    if tag == "email":
        # Faker produces realistic emails like john.doe@example.com
        return f.email()
    if tag == "phone":
        return f.phone_number()
    if tag == "street":
        return f.street_address()
    if tag == "address":
        return f.address().replace("\n", ", ")
    if tag == "city":
        return f.city()
    if tag == "state":
        return f.state()
    if tag == "state_code":
        return f.state_abbr()
    if tag == "zipcode":
        return f.zipcode()
    if tag == "country":
        return f.country()
    if tag == "country_code":
        return f.country_code()
    if tag == "latitude":
        return float(f.latitude())
    if tag == "longitude":
        return float(f.longitude())

    # ── Organisation ───────────────────────────────────────────────────────
    if tag == "company":
        return f.company()
    if tag == "department":
        return random.choice([
            "Engineering", "Sales", "Marketing", "Finance", "Human Resources",
            "Operations", "Product Management", "Customer Success", "Legal",
            "Compliance", "IT", "Research & Development", "Data Science",
            "Business Development", "Procurement", "Quality Assurance",
            "Security", "Strategy", "Accounting", "Supply Chain"
        ])
    if tag == "job_title":
        return f.job()
    if tag == "employee_type":
        return random.choice(["FULL_TIME", "PART_TIME", "CONTRACT", "TEMPORARY", "INTERN", "CONSULTANT"])
    if tag == "industry":
        return random.choice([
            "Technology", "Finance", "Healthcare", "Manufacturing", "Retail",
            "Education", "Government", "Energy", "Transportation", "Real Estate",
            "Media", "Telecommunications", "Agriculture", "Construction", "Insurance"
        ])

    # ── Finance ────────────────────────────────────────────────────────────
    if tag == "money":
        return round(random.uniform(0.01, 999999.99), 2)
    if tag == "currency":
        return random.choice(["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "SGD"])
    if tag == "account_number":
        return f.bban()
    if tag == "credit_card":
        return f.credit_card_number()
    if tag == "card_type":
        return f.credit_card_provider()
    if tag == "bank_name":
        return random.choice([
            "JPMorgan Chase", "Bank of America", "Wells Fargo", "Citibank",
            "Goldman Sachs", "Morgan Stanley", "HSBC", "Barclays", "Deutsche Bank",
            "BNP Paribas", "UBS", "Credit Suisse", "Standard Chartered", "DBS Bank",
            "US Bank", "PNC Bank", "Capital One", "TD Bank", "Santander", "ING"
        ])
    if tag == "routing_number":
        return f.aba()
    if tag == "txn_id":
        return f"TXN{f.numerify('############')}"
    if tag == "invoice_number":
        return f"INV-{datetime.date.today().year}-{f.numerify('######')}"
    if tag == "order_number":
        return f"ORD-{f.numerify('########')}"
    if tag == "sku":
        return f"{f.lexify('???', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}-{f.numerify('######')}"

    # ── Dates & Times ──────────────────────────────────────────────────────
    if tag == "datetime_past":
        return f.date_time_between(start_date="-5y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    if tag == "datetime_recent":
        return f.date_time_between(start_date="-1y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    if tag == "datetime_nullable":
        if random.random() < 0.85:
            return None
        return f.date_time_between(start_date="-2y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
    if tag == "date_past":
        return f.date_between(start_date="-5y", end_date="today").isoformat()
    if tag == "date_future":
        return f.date_between(start_date="today", end_date="+3y").isoformat()
    if tag == "time":
        return f.time()
    if tag == "year":
        return random.randint(2010, 2025)
    if tag == "month":
        return random.randint(1, 12)

    # ── Status / Flags ─────────────────────────────────────────────────────
    if tag == "status":
        return random.choice(["ACTIVE", "INACTIVE", "PENDING", "SUSPENDED", "CANCELLED", "COMPLETED"])
    if tag == "boolean":
        return random.choice(["Y", "N"])
    if tag == "category":
        return random.choice(["Electronics", "Clothing", "Food & Beverage", "Software",
                               "Services", "Hardware", "Automotive", "Healthcare", "Finance"])
    if tag == "priority":
        return random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL", "URGENT"])
    if tag == "severity":
        return random.choice(["S1", "S2", "S3", "S4"])
    if tag == "level":
        return random.choice(["L1", "L2", "L3", "L4", "L5"])

    # ── System / IT ────────────────────────────────────────────────────────
    if tag == "ipv4":
        return f.ipv4()
    if tag == "ipv6":
        return f.ipv6()
    if tag == "mac_address":
        return f.mac_address()
    if tag == "url":
        return f.url()
    if tag == "username":
        return f.user_name()
    if tag == "password_hash":
        return hashlib.sha256(f.password().encode()).hexdigest()
    if tag == "token":
        return f.sha256()
    if tag == "session_id":
        return str(uuid.uuid4()).replace("-", "")
    if tag == "api_key":
        return f"ak_{f.sha1()}"
    if tag == "log_level":
        return random.choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"])
    if tag == "http_method":
        return random.choice(["GET", "POST", "PUT", "PATCH", "DELETE"])
    if tag == "http_status":
        return random.choice([200, 201, 204, 400, 401, 403, 404, 422, 500, 503])
    if tag == "platform":
        return random.choice(["iOS", "Android", "Web", "Windows", "macOS", "Linux"])
    if tag == "browser":
        return random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"])
    if tag == "os":
        return random.choice(["Windows 11", "Windows 10", "macOS Ventura", "Ubuntu 22.04",
                               "iOS 17", "Android 14", "CentOS 8"])
    if tag == "version":
        return f"{random.randint(1,10)}.{random.randint(0,20)}.{random.randint(0,99)}"
    if tag == "filename":
        return f.file_name()
    if tag == "file_size":
        return random.randint(1024, 104857600)
    if tag == "md5":
        return f.md5()
    if tag == "user_agent":
        return f.user_agent()
    if tag == "device_id":
        return f"DEV-{str(uuid.uuid4())[:8].upper()}"

    # ── Text / Content ─────────────────────────────────────────────────────
    if tag == "text":
        return f.paragraph(nb_sentences=2)
    if tag == "sentence":
        return f.sentence()
    if tag == "tags":
        pool = ["urgent", "verified", "reviewed", "archived", "priority",
                "new", "legacy", "migrated", "escalated", "pending"]
        return ",".join(random.sample(pool, random.randint(1, 3)))
    if tag == "color":
        return f.color_name()
    if tag == "language":
        return random.choice(["en", "es", "fr", "de", "ja", "zh", "pt", "it", "nl", "ko", "ar", "hi"])
    if tag == "timezone":
        return f.timezone()

    # ── Numeric ────────────────────────────────────────────────────────────
    if tag == "quantity":
        return random.randint(1, 10000)
    if tag == "percentage":
        return round(random.uniform(0, 100), 2)
    if tag == "score":
        return round(random.uniform(0, 100), 1)
    if tag == "weight":
        return round(random.uniform(0.1, 999.99), 3)
    if tag == "height":
        return round(random.uniform(0.5, 2.5), 2)
    if tag == "duration":
        return random.randint(1, 86400)

    # ── Product / Inventory ────────────────────────────────────────────────
    if tag == "product_name":
        return f"{random.choice(['Premium','Deluxe','Standard','Pro','Elite'])} {f.word().capitalize()} {random.randint(100,999)}"
    if tag == "product_category":
        return random.choice(["Electronics", "Clothing", "Food & Beverage", "Software",
                               "Services", "Hardware", "Automotive", "Healthcare"])
    if tag == "brand":
        return f.company().split()[0]
    if tag == "model":
        return f"{f.lexify('??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}-{f.numerify('####')}"
    if tag == "serial_number":
        return f"SN{f.numerify('####')}{f.lexify('????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{f.numerify('####')}"
    if tag == "barcode":
        return f.ean13()
    if tag == "stock":
        return random.randint(0, 50000)

    return None


def gen_by_dtype(col: ColumnDef) -> Any:
    """Fallback: generate a value based purely on declared data type."""
    dt = (col.data_type or "").upper().strip()

    if col.allowed_values:
        return random.choice(col.allowed_values)

    if any(x in dt for x in ["CHAR", "VARCHAR", "TEXT", "STRING", "NVARCHAR", "CLOB"]):
        max_len = min(int(col.length), 80) if col.length else 30
        return fake.lexify("?" * random.randint(max(1, max_len // 2), max_len),
                           letters="abcdefghijklmnopqrstuvwxyz0123456789")

    if any(x in dt for x in ["BIGINT", "INT", "SMALLINT", "TINYINT", "SERIAL"]):
        lo = int(float(col.min_value)) if col.min_value else 1
        hi = int(float(col.max_value)) if col.max_value else 999999
        if "TINY"  in dt: hi = min(hi, 127)
        if "SMALL" in dt: hi = min(hi, 32767)
        return random.randint(lo, hi)

    if any(x in dt for x in ["FLOAT", "DOUBLE", "DECIMAL", "REAL", "MONEY", "NUMERIC", "NUMBER"]):
        lo = float(col.min_value) if col.min_value else 0.0
        hi = float(col.max_value) if col.max_value else 99999.99
        sc = int(col.scale) if col.scale else 2
        return round(random.uniform(lo, hi), sc)

    if any(x in dt for x in ["BOOL", "BIT", "FLAG"]):
        return random.choice(["Y", "N"])

    if dt == "DATE":
        return fake.date_between(start_date="-5y", end_date="today").isoformat()

    if any(x in dt for x in ["TIMESTAMP", "DATETIME", "DATETIME2"]):
        return fake.date_time_between(start_date="-5y", end_date="now").strftime("%Y-%m-%d %H:%M:%S")

    if any(x in dt for x in ["UUID", "GUID", "UNIQUEIDENTIFIER"]):
        return str(uuid.uuid4())

    if "JSON" in dt:
        return f'{{"id": {random.randint(1, 9999)}, "value": "{fake.word()}"}}'

    # Default fallback
    return fake.lexify("?" * random.randint(5, 15),
                       letters="abcdefghijklmnopqrstuvwxyz0123456789")


# ─────────────────────────────────────────────────────────────────────────────
# Key Registry  (PK uniqueness, FK pools, composite keys)
# ─────────────────────────────────────────────────────────────────────────────

class KeyRegistry:
    def __init__(self):
        self._pk: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))
        self._unique: Dict[str, Dict[str, Set]] = defaultdict(lambda: defaultdict(set))
        self._composite: Dict[str, Set[tuple]] = defaultdict(set)

    def add_pk(self, table, col, val):
        self._pk[table][col].append(val)

    def pk_pool(self, table, col):
        return self._pk[table][col]

    def has_pk(self, table, col):
        return bool(self._pk[table][col])

    def add_unique(self, table, col, val):
        self._unique[table][col].add(val)

    def is_unique_taken(self, table, col, val):
        return val in self._unique[table][col]

    def add_composite(self, table, tup):
        self._composite[table].add(tup)

    def composite_exists(self, table, tup):
        return tup in self._composite[table]


# ─────────────────────────────────────────────────────────────────────────────
# Mock Data Engine
# ─────────────────────────────────────────────────────────────────────────────

class MockDataEngine:

    def __init__(self, tables: List[TableDef], row_counts: Dict[str, int],
                 null_pct: float = 0.05, seed: Optional[int] = None):
        self.tables    = {t.name.lower(): t for t in tables}
        self.row_counts = {k.lower(): v for k, v in row_counts.items()}
        self.null_pct  = null_pct
        self.registry  = KeyRegistry()
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)
        self._order = self._topo_sort()

    def _topo_sort(self):
        deps = {}
        for tname, tdef in self.tables.items():
            dep_set = set()
            for col in tdef.foreign_keys:
                if col.ref_table:
                    ref = col.ref_table.lower()
                    if ref in self.tables and ref != tname:
                        dep_set.add(ref)
            deps[tname] = dep_set

        ordered, visited, in_prog = [], set(), set()

        def visit(n):
            if n in visited or n in in_prog:
                return
            in_prog.add(n)
            for dep in deps.get(n, set()):
                visit(dep)
            in_prog.discard(n)
            visited.add(n)
            ordered.append(n)

        for t in self.tables:
            visit(t)
        return ordered

    def generate_all(self) -> Dict[str, List[Dict]]:
        results = {}
        for tname in self._order:
            if tname not in self.tables:
                continue
            n = self.row_counts.get(tname, 100)
            tdef = self.tables[tname]
            rows = self._generate_table(tdef, n)
            results[tname] = rows
        return results

    def _generate_table(self, tdef: TableDef, n: int) -> List[Dict]:
        rows = []
        pk_counters: Dict[str, int] = defaultdict(int)
        composite_groups = tdef.composite_key_groups
        composite_cols   = {col.name for cols in composite_groups.values() for col in cols}

        for i in range(n):
            row = {}

            # Regular columns
            for col in tdef.columns:
                if col.name in composite_cols:
                    continue
                row[col.name] = self._gen_col(col, tdef.name, i, pk_counters)

            # Composite key columns
            for grp_cols in composite_groups.values():
                for attempt in range(200):
                    combo = {col.name: self._gen_col(col, tdef.name, i, pk_counters)
                             for col in grp_cols}
                    tup = tuple(combo[c.name] for c in grp_cols)
                    if not self.registry.composite_exists(tdef.name.lower(), tup):
                        self.registry.add_composite(tdef.name.lower(), tup)
                        row.update(combo)
                        break
                else:
                    for col in grp_cols:
                        row[col.name] = f"{i}_{random.randint(0, 99999)}"

            rows.append(row)

            # Register PKs so child tables can reference them
            for pk_col in tdef.primary_keys:
                if row.get(pk_col.name) is not None:
                    self.registry.add_pk(tdef.name.lower(), pk_col.name.lower(), row[pk_col.name])

        return rows

    def _gen_col(self, col: ColumnDef, table: str, row_idx: int,
                 pk_counters: Dict[str, int]) -> Any:

        # ── Primary key ───────────────────────────────────────────────────
        if col.is_primary_key:
            pk_counters[col.name] += 1
            val = self._gen_pk(col, pk_counters[col.name] - 1)
            self.registry.add_unique(table.lower(), col.name.lower(), val)
            return val

        # ── Foreign key ───────────────────────────────────────────────────
        if col.is_foreign_key and col.ref_table:
            val = self._resolve_fk(col)
            if val is not None:
                return val
            if col.nullable:
                return None

        # ── Allowed values (enum) ─────────────────────────────────────────
        if col.allowed_values:
            val = random.choice(col.allowed_values)
            if col.nullable and random.random() < self.null_pct:
                return None
            return val

        # ── Semantic generation ───────────────────────────────────────────
        tag = detect_semantic(col.name)
        val = generate_value(tag, row_idx, col.name) if tag else None

        # ── Data type fallback ────────────────────────────────────────────
        if val is None:
            val = gen_by_dtype(col)

        # ── Unique constraint ─────────────────────────────────────────────
        if col.is_unique and val is not None:
            for _ in range(500):
                if not self.registry.is_unique_taken(table.lower(), col.name.lower(), val):
                    break
                val = generate_value(tag, row_idx + random.randint(1, 9999), col.name) if tag else gen_by_dtype(col)
            self.registry.add_unique(table.lower(), col.name.lower(), val)

        # ── NULL injection ────────────────────────────────────────────────
        if col.nullable and val is not None and random.random() < self.null_pct:
            return None

        return val

    def _gen_pk(self, col: ColumnDef, counter: int) -> Any:
        dt = (col.data_type or "").upper()
        if any(x in dt for x in ["UUID", "GUID", "UNIQUEIDENTIFIER"]):
            return str(uuid.uuid4())
        if any(x in dt for x in ["INT", "BIGINT", "SERIAL", "SMALLINT", "NUMERIC", "NUMBER"]):
            return counter + 1
        if any(x in dt for x in ["CHAR", "VARCHAR", "TEXT", "STRING"]):
            prefix = re.sub(r"[^A-Z]", "", col.name.upper())[:4] or "ID"
            return f"{prefix}{counter + 1:08d}"
        return counter + 1

    def _resolve_fk(self, col: ColumnDef) -> Any:
        ref_tbl = col.ref_table.lower() if col.ref_table else None
        ref_col = col.ref_column.lower() if col.ref_column else None
        if not ref_tbl:
            return None
        if ref_col and self.registry.has_pk(ref_tbl, ref_col):
            pool = self.registry.pk_pool(ref_tbl, ref_col)
            if pool:
                return random.choice(pool)
        if ref_tbl in self.tables:
            for pk_col in self.tables[ref_tbl].primary_keys:
                if self.registry.has_pk(ref_tbl, pk_col.name.lower()):
                    pool = self.registry.pk_pool(ref_tbl, pk_col.name.lower())
                    if pool:
                        return random.choice(pool)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(rows: List[Dict], path: str):
    if not rows:
        Path(path).touch()
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()),
                                quoting=csv.QUOTE_NONNUMERIC, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_row_counts(raw: str, table_names: List[str]) -> Dict[str, int]:
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
    parser = argparse.ArgumentParser(
        description="Generate realistic mock data from a Collibra Excel schema using Faker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("--schema",  required=True, help="Path to Excel schema file (.xlsx)")
    parser.add_argument("--rows",    default="100",
                        help="Rows per table. e.g. 1000  or  TABLE1:500,TABLE2:2000")
    parser.add_argument("--output",  default="./output", help="Output folder (default: ./output)")
    parser.add_argument("--seed",    type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--null-pct", type=float, default=0.05,
                        help="NULL probability for nullable columns (default: 0.05 = 5%%)")
    parser.add_argument("--list-tables", action="store_true",
                        help="List tables found in schema then exit")
    args = parser.parse_args()

    print(f"\n{'='*60}\n  Mock Data Generator  (Faker-powered)\n{'='*60}")

    # Parse schema
    print(f"\n[1/3] Reading schema: {args.schema}")
    try:
        tables = parse_excel(args.schema)
    except Exception as e:
        print(f"\n  ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Found {len(tables)} table(s):")
    for t in tables:
        pks = [c.name for c in t.primary_keys]
        fks = [f"{c.name} -> {c.ref_table}" for c in t.foreign_keys]
        parts = []
        if pks: parts.append(f"PK: {pks}")
        if fks: parts.append(f"FK: {fks}")
        suffix = ("  |  " + "  ".join(parts)) if parts else ""
        print(f"    • {t.name}  ({len(t.columns)} columns{suffix})")

    if args.list_tables:
        sys.exit(0)

    # Generate
    row_counts = parse_row_counts(args.rows, [t.name for t in tables])
    print(f"\n[2/3] Generating data:")
    for t in tables:
        print(f"    • {t.name}: {row_counts.get(t.name.lower(), 100):,} rows")

    import time
    start = time.time()
    engine = MockDataEngine(tables=tables, row_counts=row_counts,
                             null_pct=args.null_pct, seed=args.seed)
    try:
        all_data = engine.generate_all()
    except Exception as e:
        print(f"\n  ERROR: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)

    elapsed = time.time() - start
    total   = sum(len(v) for v in all_data.values())
    print(f"\n  Done — {total:,} rows in {elapsed:.2f}s")

    # Write CSVs
    os.makedirs(args.output, exist_ok=True)
    print(f"\n[3/3] Writing CSVs to: {os.path.abspath(args.output)}/")
    for tname, rows in all_data.items():
        out_path = os.path.join(args.output, f"{tname}.csv")
        write_csv(rows, out_path)
        kb = os.path.getsize(out_path) / 1024
        print(f"    • {tname}.csv  —  {len(rows):,} rows  ({kb:.1f} KB)")

    print(f"\n{'='*60}\n  All done! Output: {os.path.abspath(args.output)}\n{'='*60}\n")


if __name__ == "__main__":
    main()
