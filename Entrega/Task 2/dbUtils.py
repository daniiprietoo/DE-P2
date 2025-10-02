import sqlalchemy as alc
import pandas as pd
from config import cfg

def get_connection():
    url = cfg.get_dbConnection()
    return alc.create_engine(url)

def createDB():
    if checkDBSctructure():
        print(f"[INFO] Database {cfg.db_name} already created with {cfg.db_script}")
        print("[INFO] Skipping to data processing...")
    else:
        try:
            engine = get_connection()
            print(f"[INFO] Connection to {cfg.db_host}/{cfg.db_name} for user {cfg.db_user} established successfully.")       
            with engine.begin() as conn:
                dataSQL = open(cfg.db_script).read()
                statements = dataSQL.split(";")
                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt:
                        conn.execute(alc.text(stmt))
            engine.dispose()
            print(f"[INFO] Database {cfg.db_name} created succesfully")     
        except Exception as ex:
            print("[ERROR] An error ocurred: \n", ex)

def extractTable(table):
    try:
        engine = get_connection()
        with engine.begin() as conn:
            alcTable = alc.Table(table, alc.MetaData(), autoload_with=conn)
            result = conn.execute(alc.select(alcTable))
            rows = [dict(row._mapping) for row in result]
        engine.dispose()
        return rows
    except Exception as ex:
        print(f"[ERROR] An error ocurred extracting table {table}: \n", ex)

def extractValues(table_name, target_cols, conditions):
    try:
        engine = get_connection()
        with engine.begin() as conn:
            metadata = alc.MetaData()
            table = alc.Table(table_name, metadata, autoload_with=conn)
            if isinstance(target_cols, str):
                target_cols = [target_cols]
            stmt = alc.select(*[table.c[col] for col in target_cols])
            for col, val in conditions.items():
                stmt = stmt.where(table.c[col] == val)
            result = conn.execute(stmt).fetchone()
        engine.dispose()
        if result:
            if len(target_cols) == 1:
                return result[0]
            else:
                return dict(zip(target_cols, result))
        return None
    except Exception as ex:
        print(f"[ERROR] An error ocurred extracting data from table {table_name} with conditions {conditions}: \n", ex) 

def insertData(data, batch_size=200000):
    try:
        engine = get_connection()
        metadata = alc.MetaData()
        with engine.begin() as conn:
            inspector = alc.inspect(conn)
            for table_name in inspector.get_table_names():
                if table_name in data:
                    print(f"[INFO] Inserting values into table '{table_name}' ({len(data[table_name])} records)")
                    table = alc.Table(table_name, metadata, autoload_with=conn)
                    values = data[table_name]
                    if isinstance(values, dict):
                        values = [values]

                    total = len(values)
                    for i in range(0, total, batch_size):
                        batch = values[i:i + batch_size]
                        if batch:
                            try:
                                conn.execute(table.insert(), batch)
                                print(f"[INFO] Inserted batch {i}-{i + len(batch) - 1} of {total} records")
                            except Exception as ex:
                                print(f"[ERROR] Failed to insert batch {i}-{i + len(batch) - 1}")
                                print(f"[ERROR] Batch sample: {batch[:3]} ...")
                                raise RuntimeError(f"Insert failed for table '{table_name}' on batch {i}-{i + len(batch) - 1}: {ex}") from ex

        engine.dispose()
        print("[INFO] All data inserted successfully")

    except Exception as ex:
        print("[ERROR] Error during insert:", ex)
        raise

def checkDB(engine):
    inspector = alc.inspect(engine)
    print("[INFO] Current DB:", engine.url.database)
    for table_name in inspector.get_table_names():
        print("[INFO] Table:", table_name)

def checkDBSctructure():
    try:
        engine = get_connection()
        metadata = alc.MetaData()
        metadata.reflect(bind=engine)
        schema = {}
        for table_name, table in metadata.tables.items():
            schema[table_name] = [col.name for col in table.columns]
        modelStructure = cfg.db_Skeleton()
        if schema.keys() != modelStructure.keys():
            return False
        for key in schema:
            if set(schema[key]) != set(modelStructure[key]):
                return False
        return True
    except Exception as ex:
        url = cfg.get_dbConnection()
        print(f"[ERROR] An error ocurred checking {url}: \n", ex)

