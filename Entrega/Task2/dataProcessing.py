import pandas as pd
from pandas.util import hash_pandas_object as hash
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
            print(old_name, new_name)
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

    # os.remove(file2)
    print(f"[INFO] Merge completed. Saved file in {output_file}")

    return output_file


def splitDate(date, part):
    dates = pd.to_datetime(date, errors="coerce", dayfirst=True)
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
    print(f"[DEBUG] Available columns in DataFrame: {list(df.columns)}")

    for col_name, rules in table_cfg.items():
        if isinstance(rules, dict) and rules.get("autogenerated", False):
            continue
        else:
            print(f"[DEBUG] Processing column {col_name} with rules {rules}")
            temp = applyTransform(rules, df, col_name)
            if temp is not None:
                print(
                    f"[DEBUG] Successfully processed {col_name}, got {len(temp) if hasattr(temp, '__len__') else 'scalar'} values"
                )
                records[col_name] = temp
            else:
                print(f"[DEBUG] Failed to process {col_name}")
                continue

    print(f"[DEBUG] Generated records shape: {records.shape}")
    return records


def generateHash(records, table):
    if isinstance(cfg.design.get(table, {}), dict):
        hashBase = cfg.design.get(table, {}).get("hash", {}).get("base", [])
        if not hashBase:
            return records
    else:
        return records

    hash_series = hash(records[hashBase].astype(str), index=False)
    for _, rules in cfg.design.get(table, {}).items():
        if isinstance(rules, dict):
            mapping_col = rules.get("mapping", "")
            if mapping_col:
                records[mapping_col] = hash_series.astype(str)
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

        records = generateHash(generateRecords(df, table_cfg), table)

        # Only add non-empty records
        if not records.empty:
            result[table].extend(records.to_dict(orient="records"))

    return result


def mapFact(df, result, dimension):
    for table, table_cfg in cfg.tables.items():
        if table not in result:
            continue
        df_aux = df.copy()
        records = pd.DataFrame(index=df.index)
        for col_name, rules in table_cfg.items():
            if isinstance(rules, dict):
                mapping = rules.get("value", False)
                fk = rules.get("fk", False)
            else:
                mapping = rules
                fk = False
            if mapping:
                # Check if transformation is needed (date, day, hour, duration_sec)
                if isinstance(rules, dict) and any(
                    rules.get(k, False) for k in ("date", "day", "hour", "duration_sec")
                ):
                    records[col_name] = applyTransform(rules, df, col_name)
                elif mapping in df.columns:
                    records[col_name] = df[mapping]
                else:
                    records[col_name] = None

            elif fk:
                if fk in dimension and dimension[fk]:
                    dim_df = pd.DataFrame(dimension[fk]).copy()
                    idKey = cfg.idMapping[fk]
                    if isinstance(cfg.tables.get(fk).get(idKey), dict):
                        if cfg.tables.get(fk).get(idKey).get("autogenerated", False):
                            mergeKeys = cfg.design[fk].get("hash").get("base")
                        else:
                            mergeKeys = [col for col in dim_df.columns if col != idKey]
                    else:
                        mergeKeys = [col for col in dim_df.columns if col != idKey]
                    dfKeys = []
                    for col in mergeKeys:
                        if col in cfg.tables[fk]:
                            if isinstance(cfg.tables[fk].get(col, {}), dict):
                                dim_df[col] = applyTransform(
                                    cfg.tables[fk].get(col, {}), dim_df, col
                                )
                                dfKeys.append(cfg.tables[fk].get(col, {}).get("value"))
                            else:
                                dfKeys.append(cfg.tables[fk].get(col, {}))
                    if mergeKeys:
                        # Convert data types to ensure compatibility
                        for left_key, right_key in zip(dfKeys, mergeKeys):
                            if (
                                left_key in df_aux.columns
                                and right_key in dim_df.columns
                            ):
                                # Convert both sides to string for consistent merging
                                df_aux[left_key] = df_aux[left_key].astype(str)
                                dim_df[right_key] = dim_df[right_key].astype(str)

                        merged = df_aux.merge(
                            dim_df,
                            how="left",
                            left_on=dfKeys,
                            right_on=mergeKeys,
                            suffixes=("", "_dim"),
                        )
                        records[col_name] = merged.set_index(df.index)[idKey]
                    else:
                        records[col_name] = None
                else:
                    records[col_name] = None
            else:
                records[col_name] = None
        result.setdefault(table, [])
        result[table].extend(records.to_dict(orient="records"))

    return result


def extractAutogeneratedDimensions(result):
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
                            print(
                                f"[DEBUG] Extracted {len(dimension[fk])} records from dimension table {fk}"
                            )
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
                    f"[DEBUG] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
                )
        print("[INFO] Dimensions processing concluded")
    elif procedure == "fact":
        result = dataStructure(procedure)
        dimensions = extractAutogeneratedDimensions(result)
        print("[INFO] Initializing data processing for facts")
        for f in cfg.datasources:
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(f, chunksize=chunksize, low_memory=False), start=1
            ):
                result = mapFact(chunk, result, dimensions)
                total_rows += len(chunk)
                print(
                    f"[DEBUG] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
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
        dup_rule = cfg.onDuplicate.get(table, "ignore")
        id_fields = cfg.idMapping.get(table)
        records = onNull(records, table, null_rule)
        unique_records = onDuplicate(records, table, dup_rule, id_fields)
        cleaned[table] = unique_records

    print("[INFO] Data cleaning completed")
    return cleaned
