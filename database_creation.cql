DROP KEYSPACE IF EXISTS message_database;

CREATE KEYSPACE message_database WITH REPLICATION =  {
	'class': 'SimpleStrategy',
	'replication_factor': 1	
};

use message_database;
CREATE TABLE IF NOT EXISTS oru_messages (
	tstamp text,
	msgCtrlID text,
	PatID int,
	numeric map<text, float>,
	textual map<text, text>,
	PRIMARY KEY (PatID, msgCtrlID)
);

CREATE TABLE IF NOT EXISTS adt_messages (
	tstamp text,
	msgCtrlID text,
	PatID int,
	dob text,
	current boolean,
	bedInformation text,
	PRIMARY KEY (PatID)
);

CREATE TABLE IF NOT EXISTS alarm_information (
	tstamp text,
	msgCtrlID text,
	PatID int,
	reason map<text, text>,
	severness_counter map<text, int>,
	PRIMARY KEY (PatID, msgCtrlID)
);


CREATE TABLE IF NOT EXISTS patient_standard_values (
	tstamp text,
	PatID int,
	parameters map<text, frozen <tuple<float,float>>>,
	current boolean,
	PRIMARY KEY (PatId)
);

CREATE TABLE IF NOT EXISTS patient_bed_station (
	PatID int,
	bedInformation text,
	station text,
	PRIMARY KEY (PatID, bedInformation, station)
);
