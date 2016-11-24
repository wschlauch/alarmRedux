DROP KEYSPACE IF EXISTS message_database;

CREATE KEYSPACE message_database WITH REPLICATION =  {
	'class': 'SimpleStrategy',
	'replication_factor': 1	
};

use message_database;

CREATE TABLE IF NOT EXISTS oru_messages (
	receivedTime bigint,
	msgCtrlID text,
	PatID int,
	numeric map<text, frozen<tuple<float, text>>>,
	textual map<text, text>,
	bedInformation text,
	sendTime bigint,
	abnormal map<text, text>,
	visitNumber int,
	PRIMARY KEY (PatID, msgCtrlID, visitNumber)
);

CREATE TABLE IF NOT EXISTS adt_messages (
	receivedTime bigint,
	sendTime bigint,
	msgCtrlID text,
	PatID int,
	dob text,
	current boolean,
	bedInformation text,
	PRIMARY KEY (PatID)
);

CREATE TABLE IF NOT EXISTS alarm_information (
	receivedTime bigint,
	sendTime bigint,
	msgCtrlID text,
	PatID int,
	reason set<text>,
	severness_counter map<text, int>,
	PRIMARY KEY (PatID, msgCtrlID)
);


CREATE TABLE IF NOT EXISTS patient_standard_values (
	receivedTime bigint,
	sendTime bigint,
	PatID int,
	parameters map<text, frozen <tuple<float,float>>>,
	current boolean,
	PRIMARY KEY (PatId, sendTime)
);

CREATE TABLE IF NOT EXISTS patient_bed_station (
	PatID int,
	roomNr text,
	bedInformation text,
	station text,
	time bigint,
	PRIMARY KEY (PatID, bedInformation, station)
);

CREATE TABLE IF NOT EXISTS error_log (
	msgctrlid text,
	patid int,
	receivedTime bigint,
	sendTime bigint,
	msg text,
	PRIMARY KEY (patid, msgctrlid)
);