"""Row-level validation shared by batch ingestion (Excel files) and stream ingestion (API events).

Input: a DataFrame of raw STRING values (exactly as read from Excel / JSON).
Output: (clean typed DataFrame with snake_case columns, rejected DataFrame with reason codes).

Checks (first failing reason wins per row):
  TYPE_CAST        value present but cannot be cast to the declared type
  INVALID_DATE     date outside the plausible window 2020-01-01 .. 2030-12-31 or unparsable
  NULL_KEY         primary / business key missing (e.g. non-null SKU id)
  UNKNOWN_MEDICINE MedicineID not present in the medicine master (referential integrity)
  NON_POSITIVE_PRICE price or quantity <= 0
  NEGATIVE_QUANTITY  quantity < 0
  AMOUNT_MISMATCH  Net != Gross - Discount
  SENSOR_OUT_OF_RANGE physically impossible sensor value
  DUPLICATE_PK_IN_BATCH  same primary key twice inside one file / micro-batch
"""
import os
import numpy as np
import pandas as pd
from ..sources import Source, DATE_RANGE, snake

# The provided Excel workbook stores the *UTC timestamp columns as naive local time (UTC+05:00, Asia/Karachi):
# verified: every timestamp in the workbook is exactly +5h vs. the CSV export. Naive timestamps are localised
# with this zone and converted to UTC. Generator output always writes explicit ISO-8601 'Z' strings.
EXCEL_NAIVE_TZ = os.getenv("SDI_EXCEL_NAIVE_TZ", "Asia/Karachi")

GLN_COLUMNS = {"OriginPlantGLN": 13, "DestinationWarehouseGLN": 13, "WarehouseGLN": 13, "PharmacyGLN": 13,
               "HospitalGLN": 13, "GTIN14": 14, "SSCC18": 18}


def _clean_numeric_text(s: pd.Series) -> pd.Series:
    """'95060010008000016.0' -> '95060010008000016' (identifiers that Excel/JSON turned into numbers)."""
    return s.str.replace(r"\.0+$", "", regex=True)


def _to_timestamp_utc(s: pd.Series) -> pd.Series:
    has_z = s.str.contains(r"Z$|[+-]\d\d:?\d\d$", regex=True, na=False)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")
    if has_z.any():
        out[has_z] = pd.to_datetime(s[has_z], utc=True, errors="coerce", format="ISO8601")
    naive = ~has_z & s.notna()
    if naive.any():
        local = pd.to_datetime(s[naive], errors="coerce", format="ISO8601")
        out[naive] = local.dt.tz_localize(EXCEL_NAIVE_TZ, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
    return out


def cast_frame(raw: pd.DataFrame, src: Source):
    """Cast raw strings to declared types. Returns (typed_df, reason Series)."""
    raw = raw.copy()
    for col, _ in src.columns:
        if col not in raw.columns:
            raw[col] = None
    raw = raw[[c for c, _ in src.columns]]
    raw = raw.apply(lambda s: s.astype("string").str.strip().replace({"": pd.NA, "nan": pd.NA, "NaN": pd.NA,
                                                                       "None": pd.NA, "NaT": pd.NA}))
    reason = pd.Series(pd.NA, index=raw.index, dtype="string")
    detail = pd.Series(pd.NA, index=raw.index, dtype="string")
    typed = {}
    lo, hi = pd.Timestamp(DATE_RANGE["min"]), pd.Timestamp(DATE_RANGE["max"])
    for col, t in src.columns:
        s = raw[col]
        present = s.notna()
        if t == "text":
            if col in GLN_COLUMNS:
                s = _clean_numeric_text(s).str.zfill(GLN_COLUMNS[col]).where(present)
            typed[col] = s
            continue
        if t in ("bigint", "double"):
            v = pd.to_numeric(s, errors="coerce")
            bad = present & v.isna()
            if t == "bigint":
                frac = v.notna() & (v != v.round())
                bad |= frac
                v = v.round().astype("Int64")
            typed[col] = v
            code = "TYPE_CAST"
        elif t == "date":
            v = pd.to_datetime(s.str[:10], errors="coerce", format="%Y-%m-%d")
            bad = present & (v.isna() | (v < lo) | (v > hi))
            typed[col] = v.dt.date
            code = "INVALID_DATE"
        else:  # timestamp
            v = _to_timestamp_utc(s)
            vn = v.dt.tz_localize(None)
            bad = present & (v.isna() | (vn < lo) | (vn > hi))
            typed[col] = v
            code = "INVALID_DATE"
        newly = bad & reason.isna()
        reason[newly] = code
        detail[newly] = col + "=" + s[newly].astype(str)
    return pd.DataFrame(typed, index=raw.index), reason, detail


def apply_rules(df: pd.DataFrame, src: Source, known_medicines: set, reason: pd.Series, detail: pd.Series):
    for r in src.rules:
        cols = [c for c in r.columns if c in df.columns]
        if r.kind == "not_null":
            bad = df[cols].isna().any(axis=1)
            d = df[cols].isna().apply(lambda row: ",".join(row.index[row]), axis=1) if bad.any() else None
        elif r.kind == "fk_medicine":
            v = df[cols[0]]
            bad = v.notna() & ~v.isin(known_medicines)
            d = cols[0] + "=" + v.astype(str)
        elif r.kind == "positive":
            num = df[cols].apply(pd.to_numeric, errors="coerce")
            bad = (num <= 0).any(axis=1)
            d = "values<=0 in " + ",".join(cols)
        elif r.kind == "non_negative":
            num = df[cols].apply(pd.to_numeric, errors="coerce")
            bad = (num < 0).any(axis=1)
            d = "negative in " + ",".join(cols)
        elif r.kind == "range":
            v = pd.to_numeric(df[cols[0]], errors="coerce")
            bad = (v < r.params["min"]) | (v > r.params["max"])
            d = cols[0] + "=" + v.astype(str)
        elif r.kind == "equation":
            g, disc, net = (pd.to_numeric(df[c], errors="coerce") for c in cols)
            bad = (g - disc - net).abs() > r.params["tolerance"]
            d = "gross-discount!=net"
        else:
            continue
        bad = bad.fillna(False).astype(bool)
        newly = bad & reason.isna()
        if not newly.any():
            continue
        reason[newly] = r.code
        if d is not None:
            detail[newly] = d[newly] if isinstance(d, pd.Series) else d
    # duplicate primary key inside the same batch
    dup = df.duplicated(subset=list(src.pk), keep="first") & reason.isna()
    if dup.any():
        reason[dup] = "DUPLICATE_PK_IN_BATCH"
        detail[dup] = "pk=" + df.loc[dup, list(src.pk)].astype(str).agg("|".join, axis=1)
    return reason, detail


def validate(raw: pd.DataFrame, src: Source, known_medicines: set):
    """Returns clean (snake_case, typed) and rejected (raw record + reason) frames."""
    typed, reason, detail = cast_frame(raw, src)
    reason, detail = apply_rules(typed, src, known_medicines, reason, detail)
    ok = reason.isna()
    clean = typed[ok].copy()
    clean.columns = [snake(c) for c in clean.columns]
    rej = raw.loc[~ok].copy()
    rejected = pd.DataFrame({"reason_code": reason[~ok].astype(str), "reason_detail": detail[~ok].astype(str),
                             "record": rej.astype(object).where(rej.notna(), None).to_dict("records")},
                            index=rej.index)
    return clean, rejected
