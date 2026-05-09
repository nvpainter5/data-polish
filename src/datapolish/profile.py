"""Deterministic data profiler.

Given a pandas DataFrame, produce a structured profile that captures every
column's shape, statistics, and quality signals — without any AI involved.

Why no AI here:
- Profiling is cheap, deterministic, and easy to test.
- The LLM in the next step is *much* more useful when fed a compact,
  structured profile than when fed raw rows. A 50,000-row CSV becomes
  ~5 KB of JSON; that's what we hand to the model.
- Separating "facts about the data" from "decisions about the data" is
  the cleanest pipeline split.

The output is a `DatasetProfile` (pydantic model) which can be:
  - inspected programmatically (`profile.columns[0].null_pct`)
  - dumped to JSON for storage / sending to an LLM
  - validated automatically by pydantic (catches schema drift early)
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

# How many distinct values a column may have before we stop reporting top-N.
# Above this, the column is high-cardinality (street_name, address, etc.) and
# top-N tells us nothing useful.
TOP_VALUES_CARDINALITY_LIMIT = 50

# How many of the top values to keep when a column qualifies.
TOP_VALUES_KEEP = 10

# How many sample distinct values to show for every column.
SAMPLE_VALUES_KEEP = 5


# --------------------------------------------------------------------------- #
# Pydantic models — these define the JSON schema the LLM will see.
# --------------------------------------------------------------------------- #


class TopValue(BaseModel):
    value: str
    count: int


class NumericStats(BaseModel):
    min: float
    max: float
    mean: float
    median: float
    p25: float
    p75: float
    p95: float
    p99: float
    negative_count: int
    zero_count: int


class StringStats(BaseModel):
    min_length: int
    max_length: int
    mean_length: float
    # Casing patterns — important for spotting "HEAT/HOT WATER" vs
    # "Heat/Hot Water" inconsistencies.
    count_all_upper: int
    count_all_lower: int
    count_title_case: int
    # Whitespace pathologies.
    has_leading_whitespace: int
    has_trailing_whitespace: int
    has_double_spaces: int


class DatetimeStats(BaseModel):
    min_value: str
    max_value: str
    parse_failures: int
    future_dated_count: int


class OutlierFlags(BaseModel):
    """Outlier signals on a single column.

    Two flavors of numeric outliers (IQR fences and z-score), plus
    rare-value detection for low-to-mid cardinality categorical columns.
    """

    numeric_iqr_outliers: int = 0
    numeric_zscore_outliers: int = 0
    rare_categorical_count: int = 0
    rare_categorical_examples: list[str] = Field(default_factory=list)


class ColumnProfile(BaseModel):
    name: str
    dtype: str
    null_count: int
    null_pct: float
    unique_count: int
    sample_values: list[str]
    # Type-specific extras — only one will be populated per column.
    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None
    datetime_stats: DatetimeStats | None = None
    top_values: list[TopValue] | None = None
    outliers: OutlierFlags | None = None


class DatasetProfile(BaseModel):
    source_path: str
    profiled_at: str = Field(
        default_factory=lambda: datetime.now().isoformat(timespec="seconds"),
    )
    row_count: int
    column_count: int
    columns: list[ColumnProfile]


# --------------------------------------------------------------------------- #
# Per-column profiling helpers — one per data category.
# --------------------------------------------------------------------------- #


def _profile_numeric(s: pd.Series) -> NumericStats | None:
    """Return numeric stats, or None if the column is empty after dropna."""
    s_clean = s.dropna()
    if len(s_clean) == 0:
        return None

    quantiles = s_clean.quantile([0.25, 0.5, 0.75, 0.95, 0.99])
    return NumericStats(
        min=float(s_clean.min()),
        max=float(s_clean.max()),
        mean=float(s_clean.mean()),
        median=float(s_clean.median()),
        p25=float(quantiles.loc[0.25]),
        p75=float(quantiles.loc[0.75]),
        p95=float(quantiles.loc[0.95]),
        p99=float(quantiles.loc[0.99]),
        negative_count=int((s_clean < 0).sum()),
        zero_count=int((s_clean == 0).sum()),
    )


def _profile_string(s: pd.Series) -> StringStats | None:
    """Return string stats, or None if the column is empty after dropna."""
    s_clean = s.dropna().astype(str)
    if len(s_clean) == 0:
        return None

    lengths = s_clean.str.len()

    return StringStats(
        min_length=int(lengths.min()),
        max_length=int(lengths.max()),
        mean_length=round(float(lengths.mean()), 2),
        count_all_upper=int(s_clean.str.isupper().sum()),
        count_all_lower=int(s_clean.str.islower().sum()),
        count_title_case=int(s_clean.str.istitle().sum()),
        has_leading_whitespace=int(s_clean.str.startswith(" ").sum()),
        has_trailing_whitespace=int(s_clean.str.endswith(" ").sum()),
        has_double_spaces=int(
            s_clean.str.contains("  ", regex=False, na=False).sum()
        ),
    )


def _profile_datetime(s: pd.Series) -> DatetimeStats | None:
    """Return datetime stats. Tolerates string columns that look like dates."""
    parsed = pd.to_datetime(s, errors="coerce")
    # If the original was already non-null but parsing produced NaT, that's
    # a parse failure (a malformed date string).
    parse_failures = int(
        (parsed.isna() & s.notna()).sum()
    )
    valid = parsed.dropna()
    if len(valid) == 0:
        return DatetimeStats(
            min_value="",
            max_value="",
            parse_failures=parse_failures,
            future_dated_count=0,
        )

    now = pd.Timestamp.now(tz=valid.dt.tz) if valid.dt.tz else pd.Timestamp.now()
    return DatetimeStats(
        min_value=str(valid.min()),
        max_value=str(valid.max()),
        parse_failures=parse_failures,
        future_dated_count=int((valid > now).sum()),
    )


# --------------------------------------------------------------------------- #
# Top-level entry points.
# --------------------------------------------------------------------------- #


def _profile_numeric_outliers(s: pd.Series) -> OutlierFlags:
    """IQR-based and z-score based outlier counts for a numeric series."""
    s_clean = s.dropna()
    if len(s_clean) < 4:
        return OutlierFlags()

    q1 = s_clean.quantile(0.25)
    q3 = s_clean.quantile(0.75)
    iqr = q3 - q1
    if iqr > 0:
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_count = int(((s_clean < lower) | (s_clean > upper)).sum())
    else:
        iqr_count = 0

    mean = s_clean.mean()
    std = s_clean.std()
    if std and std > 0:
        z = (s_clean - mean) / std
        z_count = int((z.abs() > 3).sum())
    else:
        z_count = 0

    return OutlierFlags(
        numeric_iqr_outliers=iqr_count,
        numeric_zscore_outliers=z_count,
    )


def _profile_categorical_outliers(
    s: pd.Series, *, threshold: float = 0.001
) -> OutlierFlags:
    """Find values whose frequency falls below `threshold` of non-null rows."""
    s_clean = s.dropna()
    if len(s_clean) == 0:
        return OutlierFlags()

    counts = s_clean.value_counts()
    total = len(s_clean)
    rare_mask = (counts / total) < threshold
    rare = counts[rare_mask]
    return OutlierFlags(
        rare_categorical_count=int(len(rare)),
        rare_categorical_examples=[str(v) for v in rare.head(5).index.tolist()],
    )


def _looks_like_datetime(name: str, s: pd.Series) -> bool:
    """Decide whether to treat a column as a datetime.

    Either the dtype already says so, or the column name hints at it
    (NYC 311 stores dates as object/string until we parse them).
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return True
    name_lower = name.lower()
    return "date" in name_lower or "time" in name_lower


def profile_column(name: str, s: pd.Series) -> ColumnProfile:
    """Profile a single pandas Series into a ColumnProfile."""
    null_count = int(s.isna().sum())
    total = len(s)
    null_pct = round(100.0 * null_count / total, 2) if total else 0.0
    unique_count = int(s.nunique(dropna=True))

    sample_values = [
        str(v) for v in s.dropna().unique()[:SAMPLE_VALUES_KEEP]
    ]

    profile = ColumnProfile(
        name=name,
        dtype=str(s.dtype),
        null_count=null_count,
        null_pct=null_pct,
        unique_count=unique_count,
        sample_values=sample_values,
    )

    # Datetime check first — it can match object-dtype string columns.
    if _looks_like_datetime(name, s):
        profile.datetime_stats = _profile_datetime(s)
    elif pd.api.types.is_numeric_dtype(s):
        profile.numeric_stats = _profile_numeric(s)
    else:
        # Treat anything else as string.
        profile.string_stats = _profile_string(s)

    # Top-N values, but only for low-cardinality columns where it's useful.
    if 0 < unique_count <= TOP_VALUES_CARDINALITY_LIMIT:
        counts = s.value_counts(dropna=True).head(TOP_VALUES_KEEP)
        profile.top_values = [
            TopValue(value=str(v), count=int(c)) for v, c in counts.items()
        ]

    # Outlier flags. Numeric columns get IQR + z-score; non-numeric, non-date
    # columns of low/mid cardinality get rare-value detection. High-cardinality
    # free-text columns (think incident_address) skip both — every value is
    # "rare" by construction.
    if pd.api.types.is_numeric_dtype(s):
        profile.outliers = _profile_numeric_outliers(s)
    elif (
        not _looks_like_datetime(name, s)
        and 1 < unique_count <= 1000
    ):
        profile.outliers = _profile_categorical_outliers(s)

    return profile


def profile_dataset(df: pd.DataFrame, source_path: str = "") -> DatasetProfile:
    """Profile every column of a DataFrame into a DatasetProfile."""
    columns = [profile_column(col, df[col]) for col in df.columns]
    return DatasetProfile(
        source_path=source_path,
        row_count=len(df),
        column_count=len(df.columns),
        columns=columns,
    )


def save_profile(profile: DatasetProfile, output_path: Path) -> None:
    """Serialize a DatasetProfile to JSON on disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(profile.model_dump_json(indent=2))


# --------------------------------------------------------------------------- #
# Slim view for sending to an LLM.
# --------------------------------------------------------------------------- #


def to_cleaning_payload(
    profile: DatasetProfile,
    *,
    skip_high_null_threshold: float = 95.0,
    sample_values_keep: int = 3,
) -> dict:
    """Return a slim view of the profile, suitable for an LLM prompt.

    The full DatasetProfile is the canonical artifact (saved to disk for
    inspection, kept for audit). This function produces a *task-specific*
    view of the profile — smaller, focused on the fields a cleaning auditor
    actually needs. Splitting these concerns keeps prompt size under control
    without losing information from our archive.

    Slimming choices (each one trades context size for relevance):
      - Skip columns with null_pct > `skip_high_null_threshold`. These are
        conditional fields (taxi/bridge/road_ramp etc.), not cleaning targets.
      - Drop `null_count` (null_pct conveys it).
      - Drop numeric percentiles p25/p75/p95/p99 (rarely used for cleaning).
      - Drop sample_values when top_values is populated (redundant).
      - Cap sample_values to `sample_values_keep` items.
    """
    cols: list[dict] = []
    skipped: list[str] = []

    for c in profile.columns:
        if c.null_pct > skip_high_null_threshold:
            skipped.append(c.name)
            continue

        entry: dict = {
            "name": c.name,
            "dtype": c.dtype,
            "null_pct": c.null_pct,
            "unique_count": c.unique_count,
        }

        if c.top_values:
            entry["top_values"] = [
                {"value": tv.value, "count": tv.count} for tv in c.top_values
            ]
        elif c.sample_values:
            entry["sample_values"] = c.sample_values[:sample_values_keep]

        if c.string_stats:
            ss = c.string_stats
            entry["string_stats"] = {
                "min_length": ss.min_length,
                "max_length": ss.max_length,
                "mean_length": ss.mean_length,
                "count_all_upper": ss.count_all_upper,
                "count_title_case": ss.count_title_case,
                "has_leading_whitespace": ss.has_leading_whitespace,
                "has_trailing_whitespace": ss.has_trailing_whitespace,
                "has_double_spaces": ss.has_double_spaces,
            }

        if c.numeric_stats:
            ns = c.numeric_stats
            entry["numeric_stats"] = {
                "min": ns.min,
                "max": ns.max,
                "mean": ns.mean,
                "median": ns.median,
                "negative_count": ns.negative_count,
                "zero_count": ns.zero_count,
            }

        if c.datetime_stats:
            entry["datetime_stats"] = c.datetime_stats.model_dump()

        cols.append(entry)

    return {
        "row_count": profile.row_count,
        "column_count_total": profile.column_count,
        "column_count_in_payload": len(cols),
        "columns_skipped_for_high_null": skipped,
        "columns": cols,
    }


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text. Good enough
    for sanity-checking prompt size before sending."""
    return len(text) // 4
