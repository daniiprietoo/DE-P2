import pandas as pd
from config import cfg
import dbUtils as db
import os

def processDatasources(chunksize):
    base = ""
    for _, data in cfg.datasources.items():
        if isinstance(data, dict) and data.get("base", False):
            base = data.get("name", False)
        else:
            continue
    for key, data in cfg.datasources.items():
        if isinstance(data, dict) and (data.get("name", "") != base):
            print(f"[INFO] Merging file {key} with {base}")
            base = mergeCSV(
                data.get("name", ""),
                base,
                chunksize,
                data.get("mergeLogic", {}),
                data.get("rename", {}),
                key,
            )
    return base


def mergeCSV(file1, file2, chunksize, mergeLogic, rename={}, resultSuffix="general"):
    file1DF = pd.read_csv(file1, low_memory=False)

    for old_name, new_name in rename.items():
        if old_name in file1DF.columns:
            file1DF = file1DF.rename(columns={old_name: new_name})

    left_col = mergeLogic.get("base")
    right_col = mergeLogic.get("field")
    file1DF[right_col] = file1DF[right_col].astype(str).str.strip()

    output_file = f"result_{resultSuffix}.csv"

    for i, chunk in enumerate(
        pd.read_csv(file2, chunksize=chunksize, low_memory=False)
    ):
        chunk[left_col] = chunk[left_col].astype(str).str.strip()

        merged = chunk.merge(
            file1DF,
            left_on=left_col,
            right_on=right_col,
            how="left",
            suffixes=("_old", None),
        )

        mode = "w" if i == 0 else "a"
        header = i == 0
        merged.to_csv(output_file, mode=mode, header=header, index=False)

    os.remove(file2)
    print(f"[INFO] Merge completed. Saved file in {output_file}")

    return output_file


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


def applyTransform(rules, dataframe, col):
    if isinstance(rules, dict):
        mapping = rules.get("value", "")
        is_date = rules.get("date", False)
        is_day = rules.get("day", False)
        is_hour = rules.get("hour", False)
        is_second = rules.get("duration_sec", False)
    else:
        mapping = rules
        is_date = False
        is_day = False
        is_hour = False
        is_second = False

    def sanitize(value):
        if isinstance(value, pd.Series):
            return value.replace({"\\N": None, "": None, "None": None})
        if isinstance(value, str) and value.strip() in ("", "\\N", "None"):
            return None
        try:
            return None if pd.isna(value) else value
        except TypeError:
            return value
    if not mapping:
        return None
    if mapping in dataframe.keys():
        if is_date:
            return sanitize(splitDate(dataframe.get(mapping), col))
        elif is_day:
            result = getDayOfWeek(dataframe.get(mapping))
            return sanitize(result)
        elif is_hour:
            result = parse_hour(dataframe.get(mapping))
            return sanitize(result)
        elif is_second:
            result = parse_duration(dataframe.get(mapping))
            return sanitize(result)
        else:
            return sanitize(dataframe.get(mapping))

    elif col in dataframe.keys():
        if is_date:
            return sanitize(splitDate(dataframe.get(col), col))
        elif is_day:
            result = getDayOfWeek(dataframe.get(col))
            return sanitize(result)
        elif is_hour:
            result = parse_hour(dataframe.get(col))
            return sanitize(result)
        elif is_second:
            result = parse_duration(dataframe.get(col))
            return sanitize(result)
        else:
            return sanitize(dataframe.get(col))
    else:
        raise Exception(f"Columns {col} nor {mapping} founded on {dataframe.keys()}")

def generateRecords(df, table_cfg):
    records = pd.DataFrame()
    for col_name, rules in table_cfg.items():
        temp = applyTransform(rules, df, col_name)
        if temp is not None:
            records[col_name] = temp
        else:
            print(f"[ERROR] Failed to process {col_name}")
            continue
    return records

def mapDimensions(df, result):
    for table, table_cfg in cfg.tables.items():
        if table not in result:
            continue

        # Get the design configuration for this table
        table_design = cfg.design.get(table, {})

        # Skip if this table is not a dimension
        if isinstance(table_design, dict):
            if table_design.get("value") != "dimension":
                continue
        elif table_design != "dimension":
            continue

        records = generateRecords(df, table_cfg)
        # Only add non-empty records
        if not records.empty:
            result[table].extend(records.to_dict(orient="records"))
    return result

def extractFromDimension(df, fk_config, dimensions, cfg):
    fk_dim = fk_config.get("fk")
    if not fk_dim or fk_dim not in dimensions:
        return pd.Series([None] * len(df), index=df.index)

    dim_df = pd.DataFrame(dimensions[fk_dim]).copy()
    aux_df = pd.DataFrame(df).copy()

    # Identificador principal de la dimensión (por ejemplo, race_id)
    id_key = cfg.idMapping.get(fk_dim)
    if not id_key or id_key not in dim_df.columns:
        return pd.Series([None] * len(df), index=df.index)

    # Criterios de unión (onDuplicate.criteria)
    criteria = cfg.onDuplicateBase.get(fk_dim)
    if not criteria:
        return pd.Series([None] * len(df), index=df.index)
    if not isinstance(criteria, list):
        criteria = [criteria]
    
    criteriaLeft = [
        c if isinstance(cfg.tables[fk_dim].get(c), dict) and "value" in cfg.tables[fk_dim][c]
        else cfg.tables[fk_dim][c]
        for c in criteria
        if c in cfg.tables[fk_dim]
    ]
    criteriaRight = criteria

    for i, col_right in enumerate(criteriaRight):
        col_left = criteriaLeft[i]
        rule = cfg.tables[fk_dim].get(col_right)
        if isinstance(rule, dict) and any(
            key in rule for key in ("date", "day", "month", "hour", "duration_sec")
        ):
            aux_df[col_left] = applyTransform(rule, aux_df, col_right)

    # Merge vectorizado (izquierda → fact)
    merged = aux_df.merge(
        dim_df,
        how="left",
        left_on=criteriaLeft,
        right_on=criteriaRight,
        suffixes=("", "_dim"),
        validate="m:1",  # 1 dimensión por registro
    )

    return merged.set_index(df.index)[id_key]


def mapFact(csv, table_name, cfg, dimensions, result):

    df = pd.DataFrame(csv)
    sheet = cfg.tables[table_name]
    records = pd.DataFrame(index=df.index)

    for attr_name, rules in sheet.items():
        # --- Caso 1: mapeo directo o transformado ---
        if isinstance(rules, dict) and "value" in rules or isinstance(rules, str):
            mapping = rules["value"] if isinstance(rules, dict) else rules

            if isinstance(rules, dict) and any(
                k in rules for k in ("date", "day", "month", "hour", "duration_sec")
            ):
                records[attr_name] = applyTransform(rules, df, mapping)
            else:
                records[attr_name] = df[mapping] if mapping in df.columns else None

        # --- Caso 2: foreign key (fk → dimensión) ---
        elif isinstance(rules, dict) and "fk" in rules:
            records[attr_name] = extractFromDimension(df, rules, dimensions, cfg)
        # --- Caso 3: sin mapeo ---
        else:
            records[attr_name] = None
    result[table_name].extend(records.to_dict(orient="records"))
    return result




def extractAutogeneratedDimensions():
    dimension = {}

    # Look through all tables to find foreign key relationships
    for table_name, table_cfg in cfg.tables.items():
        table_design = cfg.design.get(table_name, {})

        # Check if this is a fact table
        is_fact = (
            isinstance(table_design, dict) and table_design.get("value") == "fact"
        ) or table_design == "fact"

        if is_fact:
            for col_name, rules in table_cfg.items():
                if isinstance(rules, dict):
                    fk = rules.get("fk", False)
                    if fk and fk not in dimension:
                        try:
                            dimension[fk] = db.extractTable(fk)
                        except Exception as ex:
                            print(f"[WARNING] Could not extract dimension {fk}: {ex}")
                            dimension[fk] = []

    return dimension


def dataStructure(procedure):
    result = {}
    for table, _ in cfg.tables.items():
        if (
            isinstance(cfg.design[table], dict)
            and cfg.design[table]["value"] == procedure
        ):
            result[table] = []
        elif cfg.design[table] == procedure:
            result[table] = []
    return result


def mapData(procedure, chunksize=1000000):
    if procedure == "dimension":
        result = dataStructure(procedure)
        print("[INFO] Initializing data processing for dimensions")
        for f in cfg.datasources:
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(f, chunksize=chunksize, low_memory=False), start=1
            ):
                result = mapDimensions(chunk, result)
                total_rows += len(chunk)
                print(
                    f"[INFO] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
                )
        print("[INFO] Dimensions processing concluded")
    elif procedure == "fact":
        result = dataStructure(procedure)
        dimensions = extractAutogeneratedDimensions()
        print("[INFO] Initializing data processing for facts")
        for f in cfg.datasources:
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(f, chunksize=chunksize, low_memory=False), start=1
            ):
                result = mapFact(chunk, "fact_qualifying", cfg, dimensions, result)
                #result = mapFact(chunk, result, dimensions)
                total_rows += len(chunk)
                print(
                    f"[INFO] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
                )
        print("[INFO] Facts processing concluded")
    return cleanData(result)


def onNull(records, table, null_rule):
    if not records:
        return records

    def normalize(value):
        if isinstance(value, str):
            stripped = value.strip()
            if stripped in ("", "\\N", "None"):
                return None
            if stripped.lower() == "nan":
                return None
            return value
        return None if pd.isna(value) else value

    records = [{k: normalize(v) for k, v in record.items()} for record in records]

    if null_rule == "ignore":
        return [
            r
            for r in records
            if all(pd.notna(v) and v not in ("", None) for v in r.values())
        ]
    elif isinstance(null_rule, dict) and "default" in null_rule:
        default_values = null_rule["default"]
        for r in records:
            for field, default in default_values.items():
                if pd.isna(r.get(field)) or r.get(field) in ("", None):
                    r[field] = default
        return records
    elif isinstance(null_rule, dict) and "substitution" in null_rule:
        criteria = null_rule.get("substitution", {})
        for r in records:
            for field, substitute in criteria.items():
                if pd.isna(r.get(field)) or r.get(field) in ("", None):
                    if substitute in r:
                        r[field] = r.get(substitute)
                    else:
                        r[field] = substitute
        return records
    else:
        raise ValueError(
            f"[ERROR] Unknown null handling rule for table {table}: {null_rule}"
        )


def onDuplicate(records, table, dup_rule, id_fields):
    if not records:
        return records

    def resolve_keys(fields):
        if not fields:
            return []
        if isinstance(fields, (list, tuple, set)):
            candidates = list(fields)
        else:
            candidates = [fields]
        return [
            field
            for field in candidates
            if any(field in record for record in records)
        ]

    dedupe_keys = resolve_keys(id_fields)
    if not dedupe_keys:
        design_cfg = cfg.design.get(table, {})
        if isinstance(design_cfg, dict):
            base_keys = design_cfg.get("hash", {}).get("base", [])
            if isinstance(base_keys, str):
                base_keys = [base_keys]
            dedupe_keys = [
                field for field in base_keys if any(field in r for r in records)
            ]

    if not dedupe_keys:
        return records

    unique_records = []
    seen = {}

    if isinstance(dup_rule, dict) and "add" in dup_rule:
        target_field = dup_rule["add"]
        for record in records:
            key = tuple(record.get(f) for f in dedupe_keys)
            if key not in seen:
                seen[key] = record.copy()
                seen[key][target_field] = float(seen[key].get(target_field, 0) or 0)
            else:
                seen[key][target_field] += float(record.get(target_field, 0) or 0)
        unique_records = list(seen.values())
    elif dup_rule in ("ignore", "update", None):
        seen_keys = set()
        for record in records:
            key = tuple(record.get(f) for f in dedupe_keys)
            if key not in seen_keys:
                seen_keys.add(key)
                unique_records.append(record)
    else:
        raise ValueError(
            f"Unknown duplicate handling rule for table {table}: {dup_rule}"
        )
    return unique_records


def cleanData(data):
    cleaned = {}
    print("[INFO] Initializing duplicate search and null resolving")

    for table, records in data.items():
        null_rule = cfg.onNull.get(table, "ignore")
        dup_rule = cfg.onDuplicateRule.get(table, "ignore")
        dup_base = cfg.onDuplicateBase.get(table)
        records = onNull(records, table, null_rule)
        unique_records = onDuplicate(records, table, dup_rule, dup_base)
        cleaned[table] = unique_records

    print("[INFO] Data cleaning completed")
    return cleaned
