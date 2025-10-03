CREATE SCHEMA IF NOT EXISTS f1_dw;
USE f1_dw;

CREATE TABLE IF NOT EXISTS dim_driver (
  driver_id INT AUTO_INCREMENT PRIMARY KEY,
  driver_nk INT NOT NULL,                       -- drivers.driverId
  driverRef VARCHAR(64) NOT NULL,
  code VARCHAR(16),
  forename VARCHAR(100),
  surname VARCHAR(100),
  nationality VARCHAR(100),
  number INT,
  UNIQUE KEY uq_driver_nk (driver_nk),
  UNIQUE KEY uq_driverRef (driverRef)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_circuit (
  circuit_id INT AUTO_INCREMENT PRIMARY KEY,
  circuit_nk INT NOT NULL,          -- circuits.circuit_id
  circuitRef VARCHAR(64) NOT NULL,
  name VARCHAR(200),
  location VARCHAR(200),
  country VARCHAR(100),
  altitude INT,
  lat DECIMAL(9,6),
  lng DECIMAL(9,6),
  UNIQUE KEY uq_circuit_nk (circuit_nk),
  UNIQUE KEY uq_circuitRef (circuitRef)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT AUTO_INCREMENT PRIMARY KEY,
  date DATE NOT NULL,
  year SMALLINT,
  month TINYINT,
  day TINYINT,
  day_of_week TINYINT,
  UNIQUE KEY uq_date (date)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS dim_race (
  race_id INT AUTO_INCREMENT PRIMARY KEY,
  race_nk INT NOT NULL,         -- races.raceId
  name VARCHAR(200),
  round INT,
  UNIQUE KEY uq_race_nk (race_nk)
) engine=InnoDB;

CREATE TABLE IF NOT EXISTS fact_pit_stop (
  pit_stop_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  race_id INT,
  driver_id INT,
  circuit_id INT,
  date_id INT,

  stop INT,
  lap INT,

  time_str VARCHAR(20),
  duration_str VARCHAR(20),
  duration_ms INT,

  UNIQUE KEY uq_fact_pit (race_id, driver_id, stop, lap),

  KEY ix_fp_race (race_id),
  KEY ix_fp_driver (driver_id),
  KEY ix_fp_circuit (circuit_id),
  KEY ix_fp_date (date_id),

  CONSTRAINT fk_fp_driver   FOREIGN KEY (driver_id)  REFERENCES dim_driver(driver_id),
  CONSTRAINT fk_fp_circuit  FOREIGN KEY (circuit_id) REFERENCES dim_circuit(circuit_id),
  CONSTRAINT fk_fp_date     FOREIGN KEY (date_id)    REFERENCES dim_date(date_id),
  CONSTRAINT fk_fp_race     FOREIGN KEY (race_id)    REFERENCES dim_race(race_id)
) engine=InnoDB;
