import dataProcessing as dp;
import dbUtils as db;
from config import cfg;

def resolveETL():
    print("[MAIN] Resolving ETL")
    db.createDB()
    cfg.datasources = [dp.processDatasources(1000000)]
    data = dp.mapData("dimension")
    print("[MAIN] Loading Dimensions data on database")
    db.insertData(data)
    data = dp.mapData("fact")
    print("[MAIN] Loading Fact data on database")
    db.insertData(data)
    print("[MAIN] ETL Resolved")

def main():
    resolveETL()

if __name__ == "__main__":
    main()
