CREATE TABLE IF NOT EXISTS flights.datedimension (
    DateID VARCHAR(100) PRIMARY KEY,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    DayOfTheWeek VARCHAR(20) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS flights.airlinesdimension (
    IataAirline VARCHAR(50) PRIMARY KEY,
    Airline VARCHAR(300) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS flights.locationdimension (
    IataAirport VARCHAR(50) PRIMARY KEY,
    Airport VARCHAR(200) NOT NULL,
    Country VARCHAR(50) NOT NULL,
    City VARCHAR(50) NOT NULL,
    State VARCHAR(50) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS flights.flight (
    DateID VARCHAR(100) NOT NULL,
    IataAirline VARCHAR(50) NOT NULL,
    ScheduledDeparture VARCHAR(50) NOT NULL,
    TailNumber VARCHAR(100) NOT NULL,
    OriginAirport VARCHAR(200) NOT NULL,
    DestinationAirport VARCHAR(100) NOT NULL,
    Distance INT NOT NULL,
    DepartureTime VARCHAR(50) NOT NULL,
    DelayDeparture VARCHAR(50) NOT NULL,
    ScheduledTime VARCHAR(50) NOT NULL,
    ScheduledArrival VARCHAR(50) NOT NULL,
    ArrivalTime VARCHAR(50) NOT NULL,
    DelayArrival VARCHAR(50) NOT NULL,

    CONSTRAINT fk_date FOREIGN KEY (DateID) REFERENCES datedimension(DateID),
    CONSTRAINT fk_airline FOREIGN KEY (IataAirline) REFERENCES airlinesdimension(IataAirline),
    CONSTRAINT fk_airportOrigin FOREIGN KEY (OriginAirport) REFERENCES locationdimension(IataAirport),
    CONSTRAINT fk_airportDestination FOREIGN KEY (DestinationAirport) REFERENCES locationdimension(IataAirport),
    PRIMARY KEY (DateID, IataAirline, TailNumber, ScheduledDeparture)
) ENGINE=InnoDB;