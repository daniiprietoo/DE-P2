CREATE TABLE IF NOT EXISTS dim_driver (
  driver_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_nk INT NOT NULL,             -- drivers.driverId in csv
  driver_ref VARCHAR(64) NOT NULL,     
  code VARCHAR(16),
  forename VARCHAR(100),
  surname VARCHAR(100),
  nationality VARCHAR(100),
  number INT,
  url VARCHAR(255),
  UNIQUE KEY uq_driver_nk (driver_nk),
  UNIQUE KEY uq_driver_ref (driver_ref)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_constructor (
  constructor_id INT AUTO_INCREMENT PRIMARY KEY,
  constructor_nk INT NOT NULL,        -- constructors.constructorId in csv
  constructor_ref VARCHAR(64) NOT NULL,
  name VARCHAR(200),
  nationality VARCHAR(100),
  url VARCHAR(255),
  UNIQUE KEY uq_constructor_nk (constructor_nk),
  UNIQUE KEY uq_constructor_ref (constructor_ref)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_circuit (
  circuit_id INT AUTO_INCREMENT PRIMARY KEY,
  circuit_nk INT NOT NULL,            -- circuits.circuitId in csv
  circuit_ref VARCHAR(64) NOT NULL,
  name VARCHAR(200),
  location VARCHAR(200),
  country VARCHAR(100),
  altitude INT,
  latitude DECIMAL(9,6),
  longitude DECIMAL(9,6),
  url VARCHAR(255),
  UNIQUE KEY uq_circuit_nk (circuit_nk),
  UNIQUE KEY uq_circuit_ref (circuit_ref)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT AUTO_INCREMENT PRIMARY KEY,
  date DATE NOT NULL,                 
  year SMALLINT,
  month TINYINT,
  day TINYINT,

  UNIQUE KEY uq_date (date)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_race (
  race_id INT AUTO_INCREMENT PRIMARY KEY,
  race_nk INT NOT NULL,               -- races.raceId in csv
  name VARCHAR(200),
  round INT,                 
  year SMALLINT,
  url VARCHAR(255),
  UNIQUE KEY uq_race_nk (race_nk)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_status (
  status_id INT AUTO_INCREMENT PRIMARY KEY,
  status_nk INT NOT NULL,           -- statuses.statusId in csv
  status_name VARCHAR(200) NOT NULL,
  UNIQUE KEY uq_status_nk (status_nk)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS fact_race_result (
  race_result_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  race_id INT,
  driver_id INT,
  constructor_id INT,
  circuit_id INT,
  date_id INT,
  status_id INT,

  initial_position INT,
  final_position INT,
  ranking_position INT,
  points DECIMAL(8,3),
  laps INT,

  time_ms INT,

  fastest_lap INT,
  fastest_lap_time_ms INT,
  fastest_lap_top_speed DECIMAL(8,3),

  UNIQUE KEY uq_fact_result (race_id, driver_id),

  KEY ix_fr_race (race_id),
  KEY ix_fr_driver (driver_id),
  KEY ix_fr_constructor (constructor_id),
  KEY ix_fr_circuit (circuit_id),
  KEY ix_fr_date (date_id),
  KEY ix_fr_status (status_id),

  CONSTRAINT fk_fr_driver      FOREIGN KEY (driver_id)      REFERENCES dim_driver(driver_id),
  CONSTRAINT fk_fr_constructor FOREIGN KEY (constructor_id) REFERENCES dim_constructor(constructor_id),
  CONSTRAINT fk_fr_circuit     FOREIGN KEY (circuit_id)     REFERENCES dim_circuit(circuit_id),
  CONSTRAINT fk_fr_date        FOREIGN KEY (date_id)        REFERENCES dim_date(date_id),
  CONSTRAINT fk_fr_race        FOREIGN KEY (race_id)        REFERENCES dim_race(race_id),
  CONSTRAINT fk_fr_status      FOREIGN KEY (status_id)      REFERENCES dim_status(status_id)
) engine=InnoDB;
