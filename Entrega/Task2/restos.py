"""
fila_csv: fila del csv
atribute_sheet: atributo de una plantilla de un record de fact(race_id: { fk: dim_race })
dimensions: lista de dicts de dimensiones precargadas

def extractFromDimension(fila_csv, atribute_sheet, dimensions):
    dimension = dimensions[atribute_sheet[fk]]
    if dimension exist:
        compareRecord = { fila_csv[design[fk].onDuplicate[criteria]] }
        for fila_dimension:
            if fila_dimension == compareRecord
                return dimension[design[fk].id]
            else continue
    return []

"""
"""
csv: filas del csv (lista de dicts)
sheet: plantilla de un record de fact
dimensions: lista de dicts de dimensiones precargadas

def mapFact(csv, sheet, dimensions):
    records = pd.DataFrame(index=df.index)
    for atribute_sheet:
        if atribute_sheet is mapped:
            if atribute_sheet need transformation:
                records[atribute_sheet] = transform(atribute_sheet)
            else:
                record[atribute_sheet] = atribute_sheet
        else if atribute_sheet is fk:
            records[atribute_sheet] = extractFromDimension(fila_csv, atribute_sheet, dimensions)
    return records                
"""
""" 
    if procedure == "dimension":
        result = dataStructure(procedure)
        print("[INFO] Initializing data processing for dimensions")
        for f in cfg.datasources:
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(f, chunksize=chunksize, low_memory=False), start=1
            ):
                result = mapRecords(chunk, result)
                total_rows += len(chunk)
                print(
                    f"[INFO] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
                )
        print("[INFO] Dimensions processing concluded")
    elif procedure == "fact":
        result = dataStructure(procedure)
        dimensions = extractFKDimensions()
        print("[INFO] Initializing data processing for facts")
        for f in cfg.datasources:
            total_rows = 0
            for i, chunk in enumerate(
                pd.read_csv(f, chunksize=chunksize, low_memory=False), start=1
            ):
                result = mapRecords(chunk, result, dimensions)
                total_rows += len(chunk)
                print(
                    f"[INFO] File: {f} | Chunk {i} processed | Rows so far: {total_rows}"
                )
        print("[INFO] Facts processing concluded")
    return cleanData(result)
    """

def mapDimensions(df, result):
    for table, table_cfg in cfg.tables.items():
        if table not in result:
            continue

        records = generateRecords(df, table_cfg)
        if not records.empty:
            result[table].extend(records.to_dict(orient="records"))
    return result

def mapFact(df, table_name, dimensions, result):
    print(result.keys())
    sheet = cfg.tables[table_name]
    records = generateRecords(df, sheet, dimensions=dimensions)
    result[table_name].extend(records.to_dict(orient="records"))
    return result

def generateRecordsN(df, table_cfg):
    records = pd.DataFrame()
    for col_name, rules in table_cfg.items():
        temp = tfm.applyTransform(rules, df, col_name)
        if temp is not None:
            records[col_name] = temp
        else:
            print(f"[ERROR] Failed to process {col_name}")
            continue
    return records

def mapFactN(csv, table_name, dimensions, result):

    df = pd.DataFrame(csv)
    sheet = cfg.tables[table_name]
    records = pd.DataFrame(index=df.index)
    for attr_name, rules in sheet.items():
        if isinstance(rules, dict) and "value" in rules or isinstance(rules, str):
            mapping = rules["value"] if isinstance(rules, dict) else rules
            records[attr_name] = tfm.applyTransform(rules, df, mapping)

        elif isinstance(rules, dict) and "fk" in rules:
            records[attr_name] = extractFromDimension(df, rules, dimensions, cfg)
        
        else:
            records[attr_name] = None
    result[table_name].extend(records.to_dict(orient="records"))
    return result


def applyTransformN(rules, dataframe, col):
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

def mapFactN(df, result, dimension):
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

def unique_column_values(csv_path, column_name):
    """
    Lee un CSV y devuelve un set con todos los valores distintos de la columna,
    incluyendo None y NaN.
    """
    df = pd.read_csv(csv_path)
    
    if column_name not in df.columns:
        raise ValueError(f"La columna '{column_name}' no existe en el CSV")
    
    # Obtenemos los valores únicos
    unique_vals = df[column_name].unique()
    
    # Convertimos a set y normalizamos NaN y None
    result = set()
    for val in unique_vals:
        if pd.isna(val):
            result.add(float('nan'))  # o 'NaN' si quieres representarlo como string
        else:
            result.add(val)
    
    return result

def mergeCSV(file1, file2, chunksize, mergeLogic, rename= {}, resultSuffix='general'):
    file1DF = pd.read_csv(file1, low_memory=False)
    output_file = f"result_{resultSuffix}.csv"
    for field, renamedField in rename.items():
        file1DF = file1DF.rename(columns={field: renamedField})

    for i, chunk in enumerate(pd.read_csv(file2, chunksize=chunksize, low_memory=False)):
        
        merged = chunk.merge(
            file1DF,
            left_on=mergeLogic.get("base"),
            right_on=mergeLogic.get("field"),
            how="inner",
            suffixes= ("_old", None)
        )
        
        mode = "w" if i == 0 else "a" 
        header = i == 0
        merged.to_csv(output_file, mode=mode, header=header, index=False)

    os.remove(file2)
    print(f"Merge completado. Archivo guardado en {output_file}")
    return output_file

def nans(csv_path, column_name):
    """
    Lee un CSV y devuelve un set con todos los valores distintos de la columna,
    incluyendo None y NaN. Además, imprime las filas donde el valor sea NaN.
    """
    df = pd.read_csv(csv_path)
    
    if column_name not in df.columns:
        raise ValueError(f"La columna '{column_name}' no existe en el CSV")
    
    # Inicializamos set de valores únicos
    result = set()
    
    # Iteramos sobre la columna
    for idx, val in df[column_name].items():
        if pd.isna(val):
            #print(f"[NaN] Fila {idx}: {df.iloc[idx].to_dict()}")
            result.add(float('nan'))  # Representamos NaN de manera consistente
        else:
            result.add(val)
    
    return result

def generateId(*args):
    raw = "-".join(str(a) for a in args)
    return hashlib.sha256(raw.encode()).hexdigest()

def split_date(date_str, param):
    if pd.isna(date_str):
        return None, None, None
    try:
        dt = pd.to_datetime(date_str, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None, None, None        
        elif param == "year":
            return int(dt.year)
        elif param == "month":
            return int(dt.month)
        elif param == "day":
            return int(dt.day)
        elif param == "DayOfTheWeek":
            return dt.weekday() + 1
        else:
            return int(dt.year), int(dt.month), int(dt.day)
    except Exception:
        return None, None, None
    
def mapDimensionsS(dataFrame, result):
    for _, row in dataFrame.iterrows():
        for table in result:
            record = extractRecord(table, row)
            if isinstance(cfg.design[table], dict):
                hashBase = {key: record[key] for key in cfg.design[table].get("hash", {}).get("base", [])}
                for _, rules in cfg.design[table].items():
                    if isinstance(rules, dict):
                        record[rules.get("mapping", '')] = generateId(hashBase)
            result[table].append(record)
    return result

def extractRecord(table, row):
    record = {}
    for col_name, rules in cfg.tables[table].items():
        if isinstance(rules, dict):
            if rules.get("autogenerated", False):
                continue
            csv_col = rules.get("value", "")
            is_date = rules.get("date", False)
            is_day = rules.get("day", False)
        else:
            csv_col = rules
            is_date = False
            is_day = False
        value = row.get(csv_col, None)  
        if is_date:
            record[col_name] = splitDate(value, col_name.lower())
        elif is_day:
            record[col_name] = getDayOfWeek(value)
        else:
            record[col_name] = value   
    return record

def obtainForeignKey(fk, row, dimension):
    if fk not in dimension:
        return None
    table_data = dimension[fk]
    conditions = extractRecord(fk, row)
    for record in table_data:
        if all(record.get(col) == val for col, val in conditions.items()):
            return record.get(cfg.idMapping[fk], None)
    return None

def mapFact(df, result, dimension):
    for table, table_cfg in cfg.tables.items():
        records = pd.DataFrame(index=df.index)
        for col_name, rules in table_cfg.items():
            if isinstance(rules, dict):
                if rules.get("autogenerated", False):
                    continue  
                mapping = rules.get("mapped", False)
                fk = rules.get("fk", False)
            else:
                mapping = rules
                fk = False

            if mapping:
                records[col_name] = df.get(mapping)
            elif fk:
                records[col_name] = df.apply(lambda row: obtainForeignKey(fk, row, dimension), axis=1)
            else:
                records[col_name] = None

        if table not in result:
            result[table] = []

        result[table].extend(records.to_dict(orient="records"))
    return result

def insertData(data):
    try:
        engine = get_connection()
        metadata = alc.MetaData()
        with engine.begin() as conn:
            inspector = alc.inspect(conn)    

            for table_name in inspector.get_table_names():
                if table_name in data:
                    print(f"Inserting values in table {table_name}")
                    table = alc.Table(table_name, metadata, autoload_with=conn)

                    values = data[table_name]
                    if isinstance(values, dict):
                        values = [values]

                    for row in values:
                        row_clean = clean(row)
                        stmt = alc.insert(table).values(row_clean)
                        #stmt = stmt.prefix_with("IGNORE")
                        conn.execute(stmt)

        engine.dispose()
        print("All data inserted")

    except Exception as ex:
        print(f"An error occurred: {ex}")    

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
