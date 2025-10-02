-- F1 Data Warehouse - Star Schemas for Analysis

-- Schema 1: Qualification Performance
-- Dimension Tables
CREATE TABLE IF NOT EXISTS f1.dim_driver (
    DriverID INT PRIMARY KEY,
    DriverRef VARCHAR(100) NOT NULL,
    DriverNumber INT,
    Code VARCHAR(10),
    Forename VARCHAR(100) NOT NULL,
    Surname VARCHAR(100) NOT NULL,
    DateOfBirth DATE,
    Nationality VARCHAR(100)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS f1.dim_constructor (
    ConstructorID INT PRIMARY KEY,
    ConstructorRef VARCHAR(100) NOT NULL,
    ConstructorName VARCHAR(200) NOT NULL,
    Nationality VARCHAR(100)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS f1.dim_circuit (
    CircuitID INT PRIMARY KEY,
    CircuitRef VARCHAR(100) NOT NULL,
    CircuitName VARCHAR(200) NOT NULL,
    Location VARCHAR(100),
    Country VARCHAR(100),
    Lat DECIMAL(10, 6),
    Lng DECIMAL(10, 6),
    Alt INT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS f1.dim_race (
    RaceID INT PRIMARY KEY,
    Year INT NOT NULL,
    Round INT NOT NULL,
    CircuitID INT NOT NULL,
    RaceName VARCHAR(200) NOT NULL,
    RaceDate DATE,
    RaceTime TIME,
    CONSTRAINT fk_race_circuit FOREIGN KEY (CircuitID) REFERENCES dim_circuit(CircuitID)
) ENGINE=InnoDB;

-- Fact Table 1: Qualification Results
CREATE TABLE IF NOT EXISTS f1.fact_qualifying (
    QualifyID INT PRIMARY KEY AUTO_INCREMENT,
    RaceID INT NOT NULL,
    DriverID INT NOT NULL,
    ConstructorID INT NOT NULL,
    QualifyNumber INT NOT NULL,
    Position INT,
    Q1 VARCHAR(20),
    Q2 VARCHAR(20),
    Q3 VARCHAR(20),
    CONSTRAINT fk_qual_race FOREIGN KEY (RaceID) REFERENCES dim_race(RaceID),
    CONSTRAINT fk_qual_driver FOREIGN KEY (DriverID) REFERENCES dim_driver(DriverID),
    CONSTRAINT fk_qual_constructor FOREIGN KEY (ConstructorID) REFERENCES dim_constructor(ConstructorID)
) ENGINE=InnoDB;

-- Schema 2: Pit Stops Analysis
-- Fact Table 2: Pit Stops
CREATE TABLE IF NOT EXISTS f1.fact_pitstops (
    PitStopID INT PRIMARY KEY AUTO_INCREMENT,
    RaceID INT NOT NULL,
    DriverID INT NOT NULL,
    Stop INT NOT NULL,
    Lap INT NOT NULL,
    PitStopTime TIME,
    Duration VARCHAR(20),
    Milliseconds INT,
    CONSTRAINT fk_pit_race FOREIGN KEY (RaceID) REFERENCES dim_race(RaceID),
    CONSTRAINT fk_pit_driver FOREIGN KEY (DriverID) REFERENCES dim_driver(DriverID)
) ENGINE=InnoDB;

-- Schema 3: Race Results Analysis
CREATE TABLE IF NOT EXISTS f1.dim_status (
    StatusID INT PRIMARY KEY,
    Status VARCHAR(200) NOT NULL
) ENGINE=InnoDB;

-- Fact Table 3: Race Results
CREATE TABLE IF NOT EXISTS f1.fact_results (
    ResultID INT PRIMARY KEY AUTO_INCREMENT,
    RaceID INT NOT NULL,
    DriverID INT NOT NULL,
    ConstructorID INT NOT NULL,
    ResultNumber INT,
    Grid INT NOT NULL,
    Position INT,
    PositionText VARCHAR(10),
    PositionOrder INT NOT NULL,
    Points DECIMAL(5, 2) NOT NULL,
    Laps INT NOT NULL,
    ResultTime VARCHAR(20),
    Milliseconds INT,
    FastestLap INT,
    FastestLapRank INT,
    FastestLapTime VARCHAR(20),
    FastestLapSpeed VARCHAR(20),
    StatusID INT NOT NULL,
    CONSTRAINT fk_result_race FOREIGN KEY (RaceID) REFERENCES dim_race(RaceID),
    CONSTRAINT fk_result_driver FOREIGN KEY (DriverID) REFERENCES dim_driver(DriverID),
    CONSTRAINT fk_result_constructor FOREIGN KEY (ConstructorID) REFERENCES dim_constructor(ConstructorID),
    CONSTRAINT fk_result_status FOREIGN KEY (StatusID) REFERENCES dim_status(StatusID)
) ENGINE=InnoDB;
