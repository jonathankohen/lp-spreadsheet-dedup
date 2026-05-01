"""
Pollstar list processor — converts raw Pollstar XLSX exports into
Mailchimp and Salesforce import CSVs.

Categories produced:
  - Agents
  - Presenters  (venues, festivals, colleges, clubs, talent buyers)
  - Artists
  - Record Labels

Each category gets two output files: Mailchimp format and Salesforce format.
"""

import io
import os
import csv
import pandas as pd
from datetime import date

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "assets", "examples", "2026 Pollstar Lists")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

TODAY = date.today().strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Source file → category mapping
# ---------------------------------------------------------------------------

PRESENTER_FILES = [
    "Colleges Fairs Festivals Theme Parks.xlsx",
    "Major Concert Venues.xlsx",
    "Night Clubs and Casinos.xlsx",
    "Talent Buyers.xlsx",
]

RECORD_LABEL_FILES = [
    "Record Label.xlsx",
]

# ---------------------------------------------------------------------------
# Lead source / tag strings written into output files
# ---------------------------------------------------------------------------

AGENT_LEAD_SOURCE = "2026 Pollstar Agents"
PRESENTER_LEAD_SOURCE = "2026 Pollstar Presenters"
ARTIST_LEAD_SOURCE = "2026 Pollstar Artists"
RECORD_LABEL_LEAD_SOURCE = "2026 Pollstar Record Labels"
ACCOUNT_SOURCE = "2026 Pollstar"


# ---------------------------------------------------------------------------
# Data loading & cleaning
# ---------------------------------------------------------------------------


def load_files(file_list: list[str]) -> pd.DataFrame:
    dfs = []
    for fname in file_list:
        path = os.path.join(INPUT_DIR, fname)
        if os.path.exists(path):
            dfs.append(pd.read_excel(path, dtype=str))
        else:
            print(f"  WARNING: {fname} not found, skipping")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_file_objects(file_objects) -> pd.DataFrame:
    """Load a list of werkzeug FileStorage (or any file-like) objects into a DataFrame."""
    dfs = []
    for f in file_objects:
        if f and f.filename and f.filename.lower().endswith(".xlsx"):
            dfs.append(pd.read_excel(f.stream, dtype=str))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def filter_no_email(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split into (with_email_df, no_email_df)."""
    has_email = df["Email"].fillna("").str.strip() != ""
    return df[has_email].reset_index(drop=True), df[~has_email].reset_index(drop=True)


def dedup_by_email(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicate contacts. Two rows are duplicates when they share the same
    non-empty email address (case-insensitive). Rows without email are kept.
    """
    df = df.copy()
    email_lower = df["Email"].fillna("").str.strip().str.lower()
    seen: set[str] = set()
    keep = []
    for email in email_lower:
        if email == "":
            keep.append(True)
        elif email in seen:
            keep.append(False)
        else:
            seen.add(email)
            keep.append(True)
    return df[keep].reset_index(drop=True)


def v(row: pd.Series, col: str) -> str:
    """Return a clean string value from a row, or empty string."""
    val = row.get(col, "")
    if pd.isna(val):
        return ""
    return str(val).strip()


def street(row: pd.Series) -> str:
    """
    Combine mailing address lines, falling back to physical address when
    mailing address is absent.  Format: "Line1, Line2" (trailing comma when
    Line2 is empty, matching the template convention).
    """
    a1 = v(row, "MailingAddress1") or v(row, "PhysicalAddress1")
    a2 = v(row, "MailingAddress2") or v(row, "PhysicalAddress2")
    return f"{a1}, {a2}" if a2 else f"{a1}, "


def format_phone(raw: str) -> str:
    """
    Best-effort normalisation to E.164-ish format.
    10-digit North-American numbers get +1 prepended.
    Already-prefixed or non-standard numbers are returned cleaned.
    """
    if not raw:
        return ""
    digits = "".join(c for c in raw if c.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits[0] == "1":
        return f"+{digits}"
    # International or unusual — return stripped original
    return raw.strip()


# ---------------------------------------------------------------------------
# Country / state normalisation
# ---------------------------------------------------------------------------

COUNTRY_CODES = {
    "afghanistan": "AFG", "albania": "ALB", "algeria": "DZA", "andorra": "AND",
    "angola": "AGO", "argentina": "ARG", "armenia": "ARM", "australia": "AUS",
    "austria": "AUT", "azerbaijan": "AZE", "bahrain": "BHR", "bangladesh": "BGD",
    "belarus": "BLR", "belgium": "BEL", "bolivia": "BOL", "bosnia and herzegovina": "BIH",
    "botswana": "BWA", "brazil": "BRA", "bulgaria": "BGR", "cambodia": "KHM",
    "cameroon": "CMR", "canada": "CAN", "chile": "CHL", "china": "CHN",
    "colombia": "COL", "costa rica": "CRI", "croatia": "HRV", "cuba": "CUB",
    "cyprus": "CYP", "czech republic": "CZE", "czechia": "CZE", "denmark": "DNK",
    "dominican republic": "DOM", "ecuador": "ECU", "egypt": "EGY", "el salvador": "SLV",
    "estonia": "EST", "ethiopia": "ETH", "finland": "FIN", "france": "FRA",
    "georgia": "GEO", "germany": "DEU", "ghana": "GHA", "greece": "GRC",
    "guatemala": "GTM", "honduras": "HND", "hong kong": "HKG", "hungary": "HUN",
    "iceland": "ISL", "india": "IND", "indonesia": "IDN", "iran": "IRN",
    "iraq": "IRQ", "ireland": "IRL", "israel": "ISR", "italy": "ITA",
    "jamaica": "JAM", "japan": "JPN", "jordan": "JOR", "kazakhstan": "KAZ",
    "kenya": "KEN", "kuwait": "KWT", "latvia": "LVA", "lebanon": "LBN",
    "lithuania": "LTU", "luxembourg": "LUX", "malaysia": "MYS", "malta": "MLT",
    "mexico": "MEX", "moldova": "MDA", "monaco": "MCO", "morocco": "MAR",
    "mozambique": "MOZ", "myanmar": "MMR", "namibia": "NAM", "nepal": "NPL",
    "netherlands": "NLD", "new zealand": "NZL", "nicaragua": "NIC", "nigeria": "NGA",
    "north korea": "PRK", "north macedonia": "MKD", "norway": "NOR", "oman": "OMN",
    "pakistan": "PAK", "panama": "PAN", "paraguay": "PRY", "peru": "PER",
    "philippines": "PHL", "poland": "POL", "portugal": "PRT", "puerto rico": "PRI",
    "qatar": "QAT", "romania": "ROU", "russia": "RUS", "russian federation": "RUS",
    "saudi arabia": "SAU", "senegal": "SEN", "serbia": "SRB", "singapore": "SGP",
    "slovakia": "SVK", "slovenia": "SVN", "south africa": "ZAF", "south korea": "KOR",
    "spain": "ESP", "sri lanka": "LKA", "sweden": "SWE", "switzerland": "CHE",
    "taiwan": "TWN", "tanzania": "TZA", "thailand": "THA", "trinidad and tobago": "TTO",
    "tunisia": "TUN", "turkey": "TUR", "türkiye": "TUR", "ukraine": "UKR",
    "united arab emirates": "ARE", "uae": "ARE", "united kingdom": "GBR",
    "uk": "GBR", "great britain": "GBR", "england": "GBR", "scotland": "GBR",
    "wales": "GBR", "united states": "USA", "united states of america": "USA",
    "us": "USA", "usa": "USA", "u.s.a.": "USA", "uruguay": "URY",
    "uzbekistan": "UZB", "venezuela": "VEN", "vietnam": "VNM", "zimbabwe": "ZWE",
    "bahamas": "BHS", "the bahamas": "BHS", "faroe islands": "FRO",
    "macedonia": "MKD", "swaziland": "SWZ", "eswatini": "SWZ",
    "republic of trinidad and tobago": "TTO",
}

STATE_CODES = {
    # US states
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    "washington dc": "DC", "washington d.c.": "DC",
    # Canadian provinces & territories
    "alberta": "AB", "british columbia": "BC", "manitoba": "MB",
    "new brunswick": "NB", "newfoundland and labrador": "NL",
    "newfoundland & labrador": "NL", "newfoundland": "NL",
    "northwest territories": "NT", "nova scotia": "NS", "nunavut": "NU",
    "ontario": "ON", "prince edward island": "PE", "quebec": "QC",
    "québec": "QC", "saskatchewan": "SK", "yukon": "YT",
}


def norm_country(val: str) -> str:
    return COUNTRY_CODES.get(val.strip().lower(), val.strip())


def norm_state(val: str) -> str:
    stripped = val.strip()
    return STATE_CODES.get(stripped.lower(), stripped)


def addr_state(row: pd.Series) -> str:
    return norm_state(v(row, "MailingState") or v(row, "PhysicalState"))


def addr_country(row: pd.Series) -> str:
    return norm_country(v(row, "MailingCountry") or v(row, "PhysicalCountry"))


# ---------------------------------------------------------------------------
# Mailchimp builders
# ---------------------------------------------------------------------------

MC_AGENT_COLS = [
    "FirstName",
    "LastName",
    "Organization Name",
    "Title",
    "Lead Source",
    "Street",
    "City",
    "State",
    "Zip/Postal code",
    "Country",
    "Email",
    "Phone",
]

MC_PRESENTER_COLS = [
    "First Name",
    "Last Name",
    "Organization Name",
    "Title",
    "Lead Source",
    "TAG",
    "Street",
    "City",
    "State",
    "Zip/Postal code",
    "Country",
    "Email",
    "Phone",
    "Capacity",
]

MC_ARTIST_COLS = [
    "FirstName",
    "LastName",
    "Organization Name",
    "Title",
    "Lead Source",
    "TAG",
    "Street",
    "City",
    "State",
    "Zip/Postal code",
    "Country",
    "Email",
    "Phone",
]

MC_RECORD_LABEL_COLS = [
    "FirstName",
    "LastName",
    "Organization Name",
    "Title",
    "Lead Source",
    "TAG",
    "Street",
    "City",
    "State",
    "Zip/Postal code",
    "Country",
    "Email",
    "Phone",
]


def build_mailchimp_agent(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "FirstName": v(r, "FirstName"),
                "LastName": v(r, "LastName"),
                "Organization Name": v(r, "Company"),
                "Title": v(r, "Title"),
                "Lead Source": lead_source,
                "Street": street(r),
                "City": v(r, "MailingCity") or v(r, "PhysicalCity"),
                "State": addr_state(r),
                "Zip/Postal code": v(r, "MailingZip") or v(r, "PhysicalZip"),
                "Country": addr_country(r),
                "Email": v(r, "Email"),
                "Phone": format_phone(v(r, "phone")),
            }
        )
    return rows


def build_mailchimp_presenter(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "First Name": v(r, "FirstName"),
                "Last Name": v(r, "LastName"),
                "Organization Name": v(r, "Company"),
                "Title": v(r, "Title"),
                "Lead Source": lead_source,
                "TAG": lead_source,
                "Street": street(r),
                "City": v(r, "MailingCity") or v(r, "PhysicalCity"),
                "State": addr_state(r),
                "Zip/Postal code": v(r, "MailingZip") or v(r, "PhysicalZip"),
                "Country": addr_country(r),
                "Email": v(r, "Email"),
                "Phone": format_phone(v(r, "phone")),
                "Capacity": v(r, "Capacity"),
            }
        )
    return rows


def build_mailchimp_artist(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "FirstName": v(r, "FirstName"),
                "LastName": v(r, "LastName"),
                "Organization Name": v(r, "Company"),
                "Title": v(r, "Title"),
                "Lead Source": lead_source,
                "TAG": lead_source,
                "Street": street(r),
                "City": v(r, "MailingCity") or v(r, "PhysicalCity"),
                "State": addr_state(r),
                "Zip/Postal code": v(r, "MailingZip") or v(r, "PhysicalZip"),
                "Country": addr_country(r),
                "Email": v(r, "Email"),
                "Phone": format_phone(v(r, "phone")),
            }
        )
    return rows


def build_mailchimp_record_label(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        rows.append(
            {
                "FirstName": v(r, "FirstName"),
                "LastName": v(r, "LastName"),
                "Organization Name": v(r, "Company"),
                "Title": v(r, "Title"),
                "Lead Source": lead_source,
                "TAG": lead_source,
                "Street": street(r),
                "City": v(r, "MailingCity") or v(r, "PhysicalCity"),
                "State": addr_state(r),
                "Zip/Postal code": v(r, "MailingZip") or v(r, "PhysicalZip"),
                "Country": addr_country(r),
                "Email": v(r, "Email"),
                "Phone": format_phone(v(r, "phone")),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Salesforce builders
# SF files use UTF-16 LE encoding (matching the template files).
# The Presenter template has a column spelled "Derpartment" — replicated here.
# ---------------------------------------------------------------------------

SF_BASE_COLS = [
    "FirstName",
    "LastName",
    "Account Name",
    "Title",
    "Account Source",
    "Shipping Street ",
    "Shipping City",
    "Shipping State",
    "Shipping Zip/ postal code",
    "Shipping Country",
    "Billing Street",
    "Billing City",
    "Billing State",
    "Billing Zip/ postal code",
    "Billing Country",
    "Mailing Street",
    "Mailing City",
    "Mailing State",
    "Mailing Zip/ postal code",
    "Mailing Country",
    "Email",
    "Phone",
]

SF_PRESENTER_COLS = (
    SF_BASE_COLS[:5]
    + ["Derpartment", "Description"]  # Capacity goes in Description
    + SF_BASE_COLS[5:]
)

SF_AGENT_COLS = SF_BASE_COLS[:5] + ["Department"] + SF_BASE_COLS[5:]
SF_ARTIST_COLS = SF_BASE_COLS[:5] + ["Department"] + SF_BASE_COLS[5:]
SF_RECORD_LABEL_COLS = SF_BASE_COLS[:5] + ["Department"] + SF_BASE_COLS[5:]


def _sf_address_block(row: pd.Series) -> dict:
    s = street(row)
    city = v(row, "MailingCity") or v(row, "PhysicalCity")
    state = addr_state(row)
    zipcode = v(row, "MailingZip") or v(row, "PhysicalZip")
    country = addr_country(row)
    return {
        "Shipping Street ": s,
        "Shipping City": city,
        "Shipping State": state,
        "Shipping Zip/ postal code": zipcode,
        "Shipping Country": country,
        "Billing Street": s,
        "Billing City": city,
        "Billing State": state,
        "Billing Zip/ postal code": zipcode,
        "Billing Country": country,
        "Mailing Street": s,
        "Mailing City": city,
        "Mailing State": state,
        "Mailing Zip/ postal code": zipcode,
        "Mailing Country": country,
    }


def build_sf_agent(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        row = {
            "FirstName": v(r, "FirstName"),
            "LastName": v(r, "LastName"),
            "Account Name": v(r, "Company"),
            "Title": v(r, "Title"),
            "Account Source": ACCOUNT_SOURCE,
            "Department": lead_source,
        }
        row.update(_sf_address_block(r))
        row["Email"] = v(r, "Email")
        row["Phone"] = format_phone(v(r, "phone"))
        rows.append(row)
    return rows


def build_sf_artist(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        row = {
            "FirstName": v(r, "FirstName"),
            "LastName": v(r, "LastName"),
            "Account Name": v(r, "Company"),
            "Title": v(r, "Title"),
            "Account Source": ACCOUNT_SOURCE,
            "Department": lead_source,
        }
        row.update(_sf_address_block(r))
        row["Email"] = v(r, "Email")
        row["Phone"] = format_phone(v(r, "phone"))
        rows.append(row)
    return rows


def build_sf_presenter(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        row = {
            "FirstName": v(r, "FirstName"),
            "LastName": v(r, "LastName"),
            "Account Name": v(r, "Company"),
            "Title": v(r, "Title"),
            "Account Source": ACCOUNT_SOURCE,
            "Derpartment": lead_source,
            "Description": v(r, "Capacity"),
        }
        row.update(_sf_address_block(r))
        row["Email"] = v(r, "Email")
        row["Phone"] = format_phone(v(r, "phone"))
        rows.append(row)
    return rows


def build_sf_record_label(df: pd.DataFrame, lead_source: str) -> list[dict]:
    rows = []
    for _, r in df.iterrows():
        row = {
            "FirstName": v(r, "FirstName"),
            "LastName": v(r, "LastName"),
            "Account Name": v(r, "Company"),
            "Title": v(r, "Title"),
            "Account Source": ACCOUNT_SOURCE,
            "Department": lead_source,
        }
        row.update(_sf_address_block(r))
        row["Email"] = v(r, "Email")
        row["Phone"] = format_phone(v(r, "phone"))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# In-memory CSV generation (for web uploads)
# ---------------------------------------------------------------------------


def _rows_to_bytes(cols: list[str], rows: list[dict], encoding: str) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode(encoding)


def generate_csvs(category: str, df: pd.DataFrame, suffix: str = "") -> dict:
    """
    Returns {filename: (bytes, mime_type)} for Mailchimp + SF outputs.
    category must be one of: agent, presenter, artist, record_label
    suffix is appended before .csv, e.g. "Phone List" → "... - Phone List.csv"
    """
    today = date.today().strftime("%Y-%m-%d")
    sfx = f" - {suffix}" if suffix else ""
    out = {}

    if category == "agent":
        mc_rows = build_mailchimp_agent(df, AGENT_LEAD_SOURCE)
        sf_rows = build_sf_agent(df, AGENT_LEAD_SOURCE)
        out[f"Mailchimp Agent List - {today}{sfx}.csv"] = (
            _rows_to_bytes(MC_AGENT_COLS, mc_rows, "utf-8-sig"),
            "text/csv",
        )
        out[f"SF Agent List - {today}{sfx}.csv"] = (
            _rows_to_bytes(SF_AGENT_COLS, sf_rows, "utf-16"),
            "text/csv",
        )

    elif category == "presenter":
        mc_rows = build_mailchimp_presenter(df, PRESENTER_LEAD_SOURCE)
        sf_rows = build_sf_presenter(df, PRESENTER_LEAD_SOURCE)
        out[f"Mailchimp Presenter List - {today}{sfx}.csv"] = (
            _rows_to_bytes(MC_PRESENTER_COLS, mc_rows, "utf-8-sig"),
            "text/csv",
        )
        out[f"SF Presenter List - {today}{sfx}.csv"] = (
            _rows_to_bytes(SF_PRESENTER_COLS, sf_rows, "utf-16"),
            "text/csv",
        )

    elif category == "artist":
        mc_rows = build_mailchimp_artist(df, ARTIST_LEAD_SOURCE)
        sf_rows = build_sf_artist(df, ARTIST_LEAD_SOURCE)
        out[f"Mailchimp Artist List - {today}{sfx}.csv"] = (
            _rows_to_bytes(MC_ARTIST_COLS, mc_rows, "utf-8-sig"),
            "text/csv",
        )
        out[f"SF Artist List - {today}{sfx}.csv"] = (
            _rows_to_bytes(SF_ARTIST_COLS, sf_rows, "utf-16"),
            "text/csv",
        )

    elif category == "record_label":
        mc_rows = build_mailchimp_record_label(df, RECORD_LABEL_LEAD_SOURCE)
        sf_rows = build_sf_record_label(df, RECORD_LABEL_LEAD_SOURCE)
        out[f"Mailchimp Record Label List - {today}{sfx}.csv"] = (
            _rows_to_bytes(MC_RECORD_LABEL_COLS, mc_rows, "utf-8-sig"),
            "text/csv",
        )
        out[f"SF Record Label List - {today}{sfx}.csv"] = (
            _rows_to_bytes(SF_RECORD_LABEL_COLS, sf_rows, "utf-16"),
            "text/csv",
        )

    return out


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------


def write_csv_utf8(path: str, cols: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows → {os.path.relpath(path, BASE_DIR)}")


def write_csv_utf16(path: str, cols: list[str], rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-16") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows):,} rows → {os.path.relpath(path, BASE_DIR)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Presenters ---
    print("Loading presenter files…")
    presenters = dedup_by_email(load_files(PRESENTER_FILES))
    print(f"  {len(presenters):,} unique presenter contacts")

    write_csv_utf8(
        os.path.join(OUTPUT_DIR, f"Mailchimp Presenter List - {TODAY}.csv"),
        MC_PRESENTER_COLS,
        build_mailchimp_presenter(presenters, PRESENTER_LEAD_SOURCE),
    )
    write_csv_utf16(
        os.path.join(OUTPUT_DIR, f"SF Presenter List - {TODAY}.csv"),
        SF_PRESENTER_COLS,
        build_sf_presenter(presenters, PRESENTER_LEAD_SOURCE),
    )

    # --- Record Labels ---
    print("\nLoading record label files…")
    labels = dedup_by_email(load_files(RECORD_LABEL_FILES))
    print(f"  {len(labels):,} unique record label contacts")

    write_csv_utf8(
        os.path.join(OUTPUT_DIR, f"Mailchimp Record Label List - {TODAY}.csv"),
        MC_RECORD_LABEL_COLS,
        build_mailchimp_record_label(labels, RECORD_LABEL_LEAD_SOURCE),
    )
    write_csv_utf16(
        os.path.join(OUTPUT_DIR, f"SF Record Label List - {TODAY}.csv"),
        SF_RECORD_LABEL_COLS,
        build_sf_record_label(labels, RECORD_LABEL_LEAD_SOURCE),
    )

    print("\nDone.")


if __name__ == "__main__":
    main()
