import pandas as pd

def splitDate(date, part):
    dates = pd.to_datetime(date, errors="coerce", dayfirst=False)
    if part.lower() == "year":
        return dates.dt.year
    elif part.lower() == "month":
        return dates.dt.month
    elif part.lower() == "day":
        return dates.dt.day
    elif part.lower() == "dayoftheweek":
        return dates.dt.weekday + 1


def getDayOfWeek(value):
    weekdays_num2str = {
        1: "Monday",
        2: "Tuesday",
        3: "Wednesday",
        4: "Thursday",
        5: "Friday",
        6: "Saturday",
        7: "Sunday",
    }
    weekdays_str2num = {v: k for k, v in weekdays_num2str.items()}

    if isinstance(value, pd.Series):
        if pd.api.types.is_numeric_dtype(value):
            return value.map(weekdays_num2str)
        else:
            return value.map(weekdays_str2num)

    if isinstance(value, int):
        return weekdays_num2str.get(value, None)
    elif isinstance(value, str):
        return weekdays_str2num.get(value, None)
    else:
        return None


def parse_duration(value):
    def to_seconds(t):
        if pd.isna(t):
            return None
        s = str(t).strip()
        if s in ("", "\\N", "None"):
            return None
        parts = s.split(":")
        if len(parts) == 3:
            h, m, s = parts
            return int(h) * 3600 + int(m) * 60 + float(s)
        elif len(parts) == 2:
            m, s = parts
            return int(m) * 60 + float(s)
        elif len(parts) == 1:
            return float(parts[0])
        else:
            return None

    if isinstance(value, pd.Series):
        return value.apply(to_seconds)
    else:
        return to_seconds(value)


def parse_hour(value):
    def to_hour(v):
        if pd.isna(v):
            return None
        s = str(v).strip()
        if s in ("", "\\N", "None"):
            return None
        try:
            t = pd.to_datetime(s, errors="coerce")
            if pd.isna(t):
                parts = s.split(":")
                return int(parts[0]) if parts and parts[0].isdigit() else None
            return t.hour
        except Exception:
            return None

    if isinstance(value, pd.Series):
        return value.apply(to_hour)
    else:
        return to_hour(value)
    
def applyTransform(rules, df, col):

    if isinstance(rules, dict):
        mapping = rules.get("value", col)
        transforms = {
            "date": rules.get("date", False),
            "day": rules.get("day", False),
            "hour": rules.get("hour", False),
            "duration_sec": rules.get("duration_sec", False)
        }
    else:
        mapping = rules
        transforms = {}

    def sanitize(series):
        if not isinstance(series, pd.Series):
            series = pd.Series(series)
        series = series.replace({"\\N": None, "": None, "None": None})
        return series.where(~series.isna(), None)

    if mapping in df.columns:
        series = df[mapping]
    elif col in df.columns:
        series = df[col]
    else:
        raise KeyError(f"Neither '{mapping}' nor '{col}' found in dataframe columns: {list(df.columns)}")

    transformers = {
        "date": lambda s: splitDate(s, col),
        "day": lambda s: getDayOfWeek(s),
        "hour": lambda s: parse_hour(s),
        "duration_sec": lambda s: parse_duration(s)
    }
    if(transforms):
        for key, active in transforms.items():
            if active:
                series = transformers[key](series)
                break

    return sanitize(series)