import yaml
from pathlib import Path
import sys

class Config:
    def __init__(self, yaml_path: str):
        self.yaml_path = Path(yaml_path)
        if not self.yaml_path.exists():
            raise FileNotFoundError(f"Config file not found: {yaml_path}")
        self._load()

    def load_mapping(self):
        mapping = {}

        for table, cols in self.tables.items():
            table_map = {}
            for col_name, rules in cols.items():
                if isinstance(rules, dict):
                    if list(rules.keys()) == ["value"]:
                        table_map[col_name] = rules["value"]
                    if list(rules.keys()) == ["mapped"]:
                        table_map[col_name] = rules["mapped"]
                    else:
                        table_map[col_name] = rules
                else:
                    table_map[col_name] = rules
            mapping[table] = table_map

        return mapping
    
    def load_config(self, config):
        onDupAux = {}
        onNullAux = {} 
        designAux = {}
        idAux = {}

        for table, configWrap in config.items():
            for configAux, param in configWrap.items():
                if(configAux == "onDuplicate"):
                    if len(param) > 1:
                        onNullAux[table] = {param["method"]: param["field"]}
                    else:
                        onNullAux[table] = param["method"]
                elif(configAux == "onNull"):
                    if len(param) > 1:
                        onNullAux[table] = {param["method"]: param["criteria"]}
                    else:
                        onNullAux[table] = param["method"]
                elif (configAux == "design"):
                    designAux[table] = param
                elif (configAux == "id"):
                    idAux[table] = param
                else:
                    raise ValueError(f"Unrecognized config for table {table} found in config file: {param}")
        return onDupAux, onNullAux, designAux, idAux

    def db_Skeleton(self):
        result = {}
        for table, columns in self.tables.items():
            attributes = list(columns.keys())
            result[table] = attributes
        return result

    def _load(self):
        with open(self.yaml_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.datasources = cfg.get("datasources", {})

        db_cfg = cfg.get("database", {})
        self.db_script = db_cfg.get("sqlScript", "")
        self.db_user = db_cfg.get("user", "")
        self.db_password = db_cfg.get("password", "")
        self.db_host = db_cfg.get("host", "")
        self.db_port = db_cfg.get("port", 0)
        self.db_name = db_cfg.get("name", "")
        self.tables = cfg.get("tables", {})
        self.tables = self.load_mapping()
        self.onDuplicate, self.onNull, self.design, self.idMapping = self.load_config(cfg.get("config", {}))

    def get_dbConnection(self):
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"

if len(sys.argv) < 2:
    raise Exception("The path of the config file must be providen")
cfg = Config(sys.argv[1])