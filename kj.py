"""
Mock Data Generator Engine
Reads Collibra-style schema from Excel, generates production-quality mock data.
"""

import random
import string
import uuid
import re
import math
import hashlib
import datetime
from collections import defaultdict, OrderedDict
from typing import Any, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Data pools for realistic production-level data
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Elizabeth","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Charles","Karen","Christopher","Lisa","Daniel","Nancy",
    "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
    "Steven","Dorothy","Paul","Kimberly","Andrew","Emily","Kenneth","Donna",
    "Joshua","Michelle","Kevin","Carol","Brian","Amanda","George","Melissa",
    "Timothy","Deborah","Ronald","Stephanie","Edward","Rebecca","Jason","Sharon",
    "Jeffrey","Laura","Ryan","Cynthia","Jacob","Kathleen","Gary","Amy","Nicholas",
    "Angela","Eric","Shirley","Jonathan","Anna","Stephen","Brenda","Larry","Pamela",
    "Justin","Emma","Scott","Nicole","Brandon","Helen","Benjamin","Samantha",
    "Samuel","Katherine","Raymond","Christine","Gregory","Debra","Frank","Rachel",
    "Alexander","Carolyn","Patrick","Janet","Jack","Catherine","Dennis","Maria",
    "Jerry","Heather","Tyler","Diane","Aaron","Julie","Jose","Joyce"
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White",
    "Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young",
    "Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores","Green",
    "Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter",
    "Roberts","Phillips","Evans","Turner","Torres","Parker","Collins","Edwards",
    "Stewart","Flores","Morris","Nguyen","Murphy","Rivera","Cook","Rogers",
    "Morgan","Peterson","Cooper","Reed","Bailey","Bell","Gomez","Kelly",
    "Howard","Ward","Cox","Diaz","Richardson","Wood","Watson","Brooks",
    "Bennett","Gray","James","Reyes","Cruz","Hughes","Price","Myers",
    "Long","Foster","Sanders","Ross","Morales","Powell","Sullivan","Russell"
]

STREET_NAMES = [
    "Main St","Oak Ave","Maple Dr","Cedar Ln","Pine Rd","Elm St","Washington Blvd",
    "Park Ave","Lake Dr","Hill Rd","River Rd","Forest Ave","Sunset Blvd",
    "Highland Ave","Valley Rd","Meadow Ln","Spring St","Willow Way","Birch St",
    "Cherry Ln","Walnut Ave","Hickory Dr","Chestnut St","Poplar Ave","Magnolia Blvd",
    "Broadway","5th Ave","Market St","Commerce Dr","Industrial Pkwy","Tech Blvd",
    "Innovation Way","Enterprise Dr","Corporate Blvd","Business Park Rd"
]

CITIES = [
    "New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia",
    "San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville",
    "Fort Worth","Columbus","Charlotte","San Francisco","Indianapolis","Seattle",
    "Denver","Washington","Nashville","Oklahoma City","El Paso","Boston",
    "Portland","Las Vegas","Memphis","Louisville","Baltimore","Milwaukee",
    "Albuquerque","Tucson","Fresno","Mesa","Sacramento","Atlanta","Kansas City",
    "Omaha","Colorado Springs","Raleigh","Long Beach","Virginia Beach","Minneapolis",
    "Tampa","New Orleans","Arlington","Wichita","Bakersfield","Aurora","Anaheim"
]

STATES = [
    ("AL","Alabama"),("AK","Alaska"),("AZ","Arizona"),("AR","Arkansas"),
    ("CA","California"),("CO","Colorado"),("CT","Connecticut"),("DE","Delaware"),
    ("FL","Florida"),("GA","Georgia"),("HI","Hawaii"),("ID","Idaho"),
    ("IL","Illinois"),("IN","Indiana"),("IA","Iowa"),("KS","Kansas"),
    ("KY","Kentucky"),("LA","Louisiana"),("ME","Maine"),("MD","Maryland"),
    ("MA","Massachusetts"),("MI","Michigan"),("MN","Minnesota"),("MS","Mississippi"),
    ("MO","Missouri"),("MT","Montana"),("NE","Nebraska"),("NV","Nevada"),
    ("NH","New Hampshire"),("NJ","New Jersey"),("NM","New Mexico"),("NY","New York"),
    ("NC","North Carolina"),("ND","North Dakota"),("OH","Ohio"),("OK","Oklahoma"),
    ("OR","Oregon"),("PA","Pennsylvania"),("RI","Rhode Island"),("SC","South Carolina"),
    ("SD","South Dakota"),("TN","Tennessee"),("TX","Texas"),("UT","Utah"),
    ("VT","Vermont"),("VA","Virginia"),("WA","Washington"),("WV","West Virginia"),
    ("WI","Wisconsin"),("WY","Wyoming")
]

COUNTRIES = [
    "United States","United Kingdom","Canada","Australia","Germany","France",
    "Japan","China","India","Brazil","Mexico","Italy","Spain","Netherlands",
    "Switzerland","Sweden","Norway","Denmark","Finland","Belgium","Austria",
    "Portugal","Poland","Czech Republic","Hungary","Romania","Greece","Turkey",
    "South Korea","Singapore","New Zealand","South Africa","Argentina","Chile"
]

COMPANY_PREFIXES = [
    "Global","National","American","United","Premier","Advanced","Strategic",
    "Integrated","Dynamic","Innovative","Digital","Smart","Elite","Prime",
    "Allied","Continental","Pacific","Atlantic","International","Enterprise"
]
COMPANY_NOUNS = [
    "Systems","Solutions","Technologies","Services","Industries","Enterprises",
    "Corporation","Group","Holdings","Partners","Associates","Ventures","Networks",
    "Consulting","Analytics","Resources","Capital","Logistics","Healthcare","Finance"
]
COMPANY_SUFFIXES = ["Inc","LLC","Corp","Ltd","Co","PLC","GmbH","SA","AG","NV"]

DEPARTMENTS = [
    "Engineering","Sales","Marketing","Finance","Human Resources","Operations",
    "Product Management","Customer Success","Legal","Compliance","IT",
    "Research & Development","Data Science","Business Development","Procurement",
    "Quality Assurance","Security","Strategy","Corporate Communications","Accounting"
]

JOB_TITLES = [
    "Software Engineer","Senior Software Engineer","Principal Engineer","Staff Engineer",
    "Product Manager","Senior Product Manager","Director of Product","VP of Product",
    "Data Scientist","Senior Data Scientist","Machine Learning Engineer",
    "DevOps Engineer","Site Reliability Engineer","Cloud Architect",
    "Sales Executive","Account Manager","Regional Sales Manager","VP of Sales",
    "Marketing Manager","Digital Marketing Specialist","Content Strategist",
    "Financial Analyst","Senior Financial Analyst","Controller","CFO",
    "HR Manager","Talent Acquisition Specialist","HR Business Partner","CHRO",
    "Operations Manager","Supply Chain Manager","Logistics Coordinator","COO",
    "Business Analyst","Systems Analyst","Solution Architect","CTO",
    "Customer Success Manager","Support Engineer","Technical Account Manager",
    "Legal Counsel","Compliance Officer","Risk Manager","General Counsel"
]

EMAIL_DOMAINS = [
    "gmail.com","yahoo.com","outlook.com","hotmail.com","icloud.com",
    "protonmail.com","aol.com","live.com","msn.com","me.com"
]

CORP_EMAIL_DOMAINS = [
    "acme.com","globex.com","initech.com","umbrella.com","cyberdyne.com",
    "weyland.com","genco.com","sterling.com","monarch.com","vehement.com",
    "hooli.com","piedpiper.com","pied-piper.com","aviato.com","dunder.com"
]

PRODUCT_ADJECTIVES = [
    "Premium","Deluxe","Standard","Essential","Professional","Enterprise",
    "Lite","Ultra","Pro","Advanced","Basic","Plus","Max","Elite","Core"
]
PRODUCT_NOUNS = [
    "Widget","Gadget","Device","Component","Module","System","Platform",
    "Solution","Service","Product","Tool","Kit","Package","Bundle","Suite"
]

CURRENCIES = ["USD","EUR","GBP","JPY","CAD","AUD","CHF","CNY","INR","BRL","MXN","SGD"]

STATUS_SETS = {
    "generic": ["ACTIVE","INACTIVE","PENDING","SUSPENDED","DELETED"],
    "order": ["PENDING","PROCESSING","SHIPPED","DELIVERED","CANCELLED","RETURNED","REFUNDED"],
    "transaction": ["INITIATED","PENDING","COMPLETED","FAILED","REVERSED","VOIDED"],
    "user": ["ACTIVE","INACTIVE","SUSPENDED","LOCKED","PENDING_VERIFICATION"],
    "account": ["OPEN","CLOSED","SUSPENDED","DORMANT","UNDER_REVIEW"],
    "ticket": ["OPEN","IN_PROGRESS","RESOLVED","CLOSED","REOPENED","ESCALATED"],
    "approval": ["DRAFT","SUBMITTED","UNDER_REVIEW","APPROVED","REJECTED","WITHDRAWN"],
    "payment": ["PENDING","AUTHORIZED","CAPTURED","SETTLED","DECLINED","REFUNDED"],
}

PHONE_AREA_CODES = [
    "201","202","203","205","206","207","208","209","210","212","213","214",
    "215","216","217","218","219","224","225","228","229","231","234","239",
    "240","248","251","252","253","254","256","260","262","267","269","270",
    "272","276","281","301","302","303","304","305","307","308","309","310",
    "312","313","314","315","316","317","318","319","320","321","323","325"
]

IP_OCTETS = lambda: f"{random.randint(1,254)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"

INDUSTRY_CODES = ["TECH","FIN","HLT","MFG","RTL","EDU","GOV","ENR","TRP","COM","AGR","CNS"]

CATEGORY_TREES = {
    "Electronics": ["Laptops","Smartphones","Tablets","Headphones","Cameras","TVs","Wearables"],
    "Clothing": ["Men's","Women's","Kids'","Footwear","Accessories","Sportswear"],
    "Food & Beverage": ["Snacks","Beverages","Dairy","Produce","Bakery","Frozen","Organic"],
    "Software": ["ERP","CRM","Analytics","Security","Productivity","Development","Cloud"],
    "Services": ["Consulting","Maintenance","Support","Training","Implementation","Managed"],
}

LOG_LEVELS = ["DEBUG","INFO","WARN","WARNING","ERROR","CRITICAL","FATAL"]
HTTP_METHODS = ["GET","POST","PUT","PATCH","DELETE","OPTIONS","HEAD"]
HTTP_STATUS_CODES = [200,201,204,301,302,400,401,403,404,409,422,429,500,502,503]
MIME_TYPES = [
    "application/json","application/xml","text/html","text/plain","text/csv",
    "application/pdf","image/jpeg","image/png","application/octet-stream"
]

BANK_NAMES = [
    "Chase","Bank of America","Wells Fargo","Citibank","US Bank","Truist",
    "PNC Bank","Goldman Sachs","Morgan Stanley","TD Bank","Capital One","Regions"
]

CARD_TYPES = ["VISA","MASTERCARD","AMEX","DISCOVER","JCB","UNIONPAY"]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _rand_str(length: int, chars: str = string.ascii_uppercase + string.digits) -> str:
    return "".join(random.choices(chars, k=length))

def _rand_digits(n: int) -> str:
    return "".join(random.choices(string.digits, k=n))

def _weighted_none(value, null_pct: float):
    """Return None with null_pct probability, else value."""
    if null_pct and random.random() < null_pct:
        return None
    return value


# ---------------------------------------------------------------------------
# Semantic name → generator mapping
# ---------------------------------------------------------------------------

def _detect_semantic(col_name: str, data_type: str) -> Optional[str]:
    """
    Infer semantic category from column name patterns.
    Returns a semantic tag used by _gen_semantic().
    """
    n = col_name.lower().replace("_", " ").replace("-", " ")

    patterns = [
        # Identity
        (r"\b(uuid|guid)\b", "uuid"),
        (r"\b(id|identifier|key)\b$", "generic_id"),

        # Person
        (r"\b(first.?name|given.?name|fname)\b", "first_name"),
        (r"\b(last.?name|surname|family.?name|lname)\b", "last_name"),
        (r"\bfull.?name\b", "full_name"),
        (r"\b(middle.?name|middle.?initial)\b", "middle_name"),
        (r"\bprefix\b", "name_prefix"),
        (r"\bsuffix\b", "name_suffix"),

        # Contact
        (r"\bemail\b", "email"),
        (r"\b(phone|mobile|cell|fax|telephone)\b", "phone"),
        (r"\b(address|addr)\b", "address_line"),
        (r"\b(street|street.?address)\b", "street"),
        (r"\b(city|town|municipality)\b", "city"),
        (r"\b(state|province|region)\b", "state"),
        (r"\b(state.?code|state.?abbr)\b", "state_code"),
        (r"\b(zip|postal|post.?code|zipcode)\b", "zipcode"),
        (r"\b(country|nation)\b", "country"),
        (r"\b(country.?code|nation.?code)\b", "country_code"),
        (r"\blatitude\b", "latitude"),
        (r"\blongitude\b", "longitude"),

        # Organization
        (r"\b(company|organization|org|firm|employer)\b", "company"),
        (r"\b(company.?name|org.?name)\b", "company"),
        (r"^(department|dept|division|unit|business.?unit)$", "department"),
        (r"\b(department|dept)\b", "department"),
        (r"\b(job.?title|position|role|designation)\b", "job_title"),
        (r"\bemployee.?type\b", "employee_type"),
        (r"\bindustry\b", "industry"),

        # Finance
        (r"\b(unit.?price|list.?price|sale.?price|sell.?price|ask.?price|bid.?price)\b", "money"),
        (r"\b(price|amount|cost|fee|charge|rate|salary|wage|revenue|income|balance|total|subtotal|tax|discount|budget|spend|earning)\b", "money"),
        (r"\b(currency|currency.?code|ccy)\b", "currency"),
        (r"\b(account.?number|acct.?num|account.?no)\b", "account_number"),
        (r"\b(card.?number|credit.?card|pan)\b", "card_number"),
        (r"\b(card.?type|card.?brand)\b", "card_type"),
        (r"\b(bank.?name|bank)\b", "bank_name"),
        (r"\b(routing.?number|aba|routing)\b", "routing_number"),
        (r"\b(transaction.?id|txn.?id|tx.?id)\b", "txn_id"),
        (r"\b(invoice.?number|invoice.?id|inv.?num)\b", "invoice_number"),
        (r"\b(order.?number|order.?id|order.?no)\b", "order_number"),
        (r"\b(sku|product.?code|item.?code)\b", "sku"),

        # Dates / Times
        (r"\b(dob|date.?of.?birth|birth.?date|birthdate)\b", "dob"),
        (r"\b(created.?at|created.?date|creation.?date|created.?on)\b", "created_at"),
        (r"\b(updated.?at|updated.?date|modified.?at|last.?modified)\b", "updated_at"),
        (r"\b(deleted.?at|deleted.?date)\b", "deleted_at"),
        (r"\b(start.?date|begin.?date|from.?date|effective.?date)\b", "start_date"),
        (r"\b(end.?date|expiry.?date|expiration.?date|expire.?date|to.?date)\b", "end_date"),
        (r"\b(date|dt)\b", "date"),
        (r"\b(time|timestamp|ts)\b", "timestamp"),
        (r"\b(year|yr)\b", "year"),
        (r"\b(month|mo)\b", "month"),
        (r"\b(day)\b", "day"),

        # Status / Flags
        (r"\b(status|state)\b", "status"),
        (r"\b(is.?\w+|has.?\w+|can.?\w+|flag)\b", "boolean"),
        (r"\bactive\b", "boolean"),
        (r"\benabled\b", "boolean"),
        (r"\bverified\b", "boolean"),
        (r"\b(type|category|kind|class|tier|level|grade)\b", "category"),
        (r"\bpriority\b", "priority"),
        (r"\bseverity\b", "severity"),

        # System
        (r"\b(ip.?address|ip.?addr|ipv4|ipv6)\b", "ip_address"),
        (r"\b(mac.?address|mac.?addr)\b", "mac_address"),
        (r"\b(url|uri|link|endpoint|website|web.?address)\b", "url"),
        (r"\b(username|user.?name|login|user.?id)\b", "username"),
        (r"\b(password|passwd|pwd|secret)\b", "password_hash"),
        (r"\btoken\b", "token"),
        (r"\b(session.?id|session)\b", "session_id"),
        (r"\b(api.?key|apikey)\b", "api_key"),
        (r"\b(log.?level|level)\b", "log_level"),
        (r"\b(http.?method|method|verb)\b", "http_method"),
        (r"\b(http.?status|status.?code|response.?code)\b", "http_status"),
        (r"\b(mime.?type|content.?type|media.?type)\b", "mime_type"),
        (r"\b(device.?id|device.?name)\b", "device_id"),
        (r"\bplatform\b", "platform"),
        (r"\bbrowser\b", "browser"),
        (r"\bos\b", "os_name"),
        (r"\bversion\b", "version"),
        (r"\b(file.?name|filename)\b", "filename"),
        (r"\b(file.?size|size)\b", "file_size"),
        (r"\b(extension|file.?ext)\b", "file_ext"),
        (r"\b(checksum|hash|md5|sha)\b", "hash"),

        # Text / Description
        (r"\b(description|desc|details|notes?|comments?|remarks?|summary|bio|about|message|body|content|text)\b", "text_long"),
        (r"\b(title|heading|headline|subject|name)\b", "title"),
        (r"\btag\b", "tags"),
        (r"\b(color|colour)\b", "color"),
        (r"\b(gender|sex)\b", "gender"),
        (r"\b(nationality|citizenship|ethnicity)\b", "nationality"),
        (r"\b(language|lang|locale)\b", "language"),
        (r"\b(timezone|time.?zone|tz)\b", "timezone"),

        # Numeric
        (r"\b(quantity|qty|count|num|number|total.?count)\b", "quantity"),
        (r"\b(percentage|percent|pct|rate|ratio)\b", "percentage"),
        (r"\b(age)\b", "age"),
        (r"\b(score|rating|rank)\b", "score"),
        (r"\b(weight)\b", "weight"),
        (r"\b(height)\b", "height"),
        (r"\b(duration|elapsed|seconds|minutes|hours)\b", "duration"),
        (r"\b(latitude|lat)\b", "latitude"),
        (r"\b(longitude|lon|lng)\b", "longitude"),

        # Product / Inventory
        (r"\b(product.?name|item.?name)\b", "product_name"),
        (r"\b(category|cat)\b", "product_category"),
        (r"\bsubcategory\b", "product_subcategory"),
        (r"\bbrand\b", "brand"),
        (r"\bmodel\b", "model"),
        (r"\bserial.?number\b", "serial_number"),
        (r"\bbarcode\b", "barcode"),
        (r"\b(stock|inventory|quantity.?on.?hand)\b", "stock"),
        (r"\b(warehouse|location.?code|bin)\b", "warehouse_code"),
    ]

    for pat, tag in patterns:
        if re.search(pat, n):
            return tag
    return None


def _gen_semantic(tag: str, row_idx: int, col_name: str) -> Any:
    """Generate a value for a given semantic tag."""
    r = random

    if tag == "uuid":
        return str(uuid.uuid4())
    if tag == "generic_id":
        return f"{col_name.upper()[:4]}-{row_idx:08d}"
    if tag == "first_name":
        return r.choice(FIRST_NAMES)
    if tag == "last_name":
        return r.choice(LAST_NAMES)
    if tag == "full_name":
        return f"{r.choice(FIRST_NAMES)} {r.choice(LAST_NAMES)}"
    if tag == "middle_name":
        return r.choice(FIRST_NAMES[:30])
    if tag == "name_prefix":
        return r.choice(["Mr.","Mrs.","Ms.","Dr.","Prof.","Rev.","Capt."])
    if tag == "name_suffix":
        return r.choice(["Jr.","Sr.","II","III","PhD","MD","Esq.","CPA",""])
    if tag == "email":
        fn = r.choice(FIRST_NAMES).lower()
        ln = r.choice(LAST_NAMES).lower()
        sep = r.choice([".","-","_",""])
        num = r.choice(["", str(r.randint(1,999))])
        dom = r.choice(EMAIL_DOMAINS + CORP_EMAIL_DOMAINS)
        return f"{fn}{sep}{ln}{num}@{dom}"
    if tag == "phone":
        formats = [
            f"+1-{r.choice(PHONE_AREA_CODES)}-{_rand_digits(3)}-{_rand_digits(4)}",
            f"({r.choice(PHONE_AREA_CODES)}) {_rand_digits(3)}-{_rand_digits(4)}",
            f"{r.choice(PHONE_AREA_CODES)}.{_rand_digits(3)}.{_rand_digits(4)}",
        ]
        return r.choice(formats)
    if tag == "address_line":
        return f"{r.randint(1,9999)} {r.choice(STREET_NAMES)}, Apt {r.randint(1,500)}"
    if tag == "street":
        return f"{r.randint(1,9999)} {r.choice(STREET_NAMES)}"
    if tag == "city":
        return r.choice(CITIES)
    if tag == "state":
        return r.choice(STATES)[1]
    if tag == "state_code":
        return r.choice(STATES)[0]
    if tag == "zipcode":
        return f"{_rand_digits(5)}-{_rand_digits(4)}" if r.random() < 0.3 else _rand_digits(5)
    if tag == "country":
        return r.choice(COUNTRIES)
    if tag == "country_code":
        codes = ["US","GB","CA","AU","DE","FR","JP","CN","IN","BR","MX","IT","ES","NL","CH"]
        return r.choice(codes)
    if tag == "latitude":
        return round(r.uniform(-90, 90), 6)
    if tag == "longitude":
        return round(r.uniform(-180, 180), 6)
    if tag == "company":
        return f"{r.choice(COMPANY_PREFIXES)} {r.choice(COMPANY_NOUNS)} {r.choice(COMPANY_SUFFIXES)}"
    if tag == "department":
        return r.choice(DEPARTMENTS)
    if tag == "job_title":
        return r.choice(JOB_TITLES)
 