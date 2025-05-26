-- Drop the database if it already exists
DROP DATABASE IF EXISTS gans ;

-- Create the database
CREATE DATABASE gans;

-- Use the database
USE gans;

-- Create the 'cities' table
CREATE TABLE cities (
	city_id INT AUTO_INCREMENT, -- auto created ID
	city VARCHAR(255) NOT NULL UNIQUE, -- name of the city
	PRIMARY KEY (city_id)
);

-- Create the 'population' table
CREATE TABLE population (
	pop_id INT AUTO_INCREMENT, -- auto created ID
	city_id INT, -- ID of the respective city
	population INT, -- Population number
	year_retrieved INT, -- year when the entry was created
	PRIMARY KEY (pop_id),
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Create the 'geo' table
CREATE TABLE geo (
	id INT AUTO_INCREMENT, -- auto created ID
	city_id INT, -- ID of the city
	longitude DECIMAL(10, 6), -- longitude of the city, + E, - W
	latitude DECIMAL(10, 6), -- latitude of the city, + N, - S
	country VARCHAR(255), -- the country of the city
	tz VARCHAR(255), -- timezone
	PRIMARY KEY (id),
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Create the 'weather' table
CREATE TABLE weather (
	id INT AUTO_INCREMENT, -- auto created ID
	city_id INT, -- ID of the city
	timestamp DATETIME, -- timestamp of weather data
	overall VARCHAR(100), -- general weather condition: clouded, rain, ...
	rain DECIMAL(10, 2), -- rain in last 3 h, in mm
	temperature DECIMAL(10, 2), -- temperature in degree Celsius
	feeled_temperature DECIMAL(10, 2), -- feeled temperature in degree C
	humidity DECIMAL(10, 2), -- humidity in percent
	windspeed DECIMAL(10, 2), -- wind speed in m/s
	clouds DECIMAL(10, 2), -- percentage of clouds
	visibility INT, -- visibility in meters, up to 10,000 m
	PRIMARY KEY (id),
	FOREIGN KEY(city_id) REFERENCES cities(city_id)
);

-- Create the 'airports' table
CREATE TABLE airports (
	id INT AUTO_INCREMENT, -- auto created ID
	city_id INT, -- ID of the city
	icao VARCHAR(4) UNIQUE, -- ICAO code
	PRIMARY KEY (id),
	FOREIGN KEY(city_id) REFERENCES cities(city_id)
);

-- Create the 'fligts' table
CREATE TABLE flights (
	id INT AUTO_INCREMENT, -- auto created ID
	icao VARCHAR(4), -- ICAO code
	arrival DATETIME, -- scheduled arrival in UTC
	fromWhere VARCHAR(255), -- name of departure airport, code not always given!
	PRIMARY KEY (id),
    FOREIGN KEY(icao) REFERENCES airports(icao)
);
