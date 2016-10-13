DROP KEYSPACE message_database;

CREATE KEYSPACE message_database WITH REPLICATION =  {
	'class': 'SimpleStrategy',
	'replication_factor': 1	
};

use message_database;
CREATE TABLE IF NOT EXISTS oru_messages (
	msgCtrlID text,
	PatID int,
	numeric map<text,int>,
	textual map<text,text>,
	PRIMARY KEY (PatID, msgCtrlID)
);

CREATE TABLE IF NOT EXISTS adt_messages (
	msgCtrlID text,
	PatID int,
	dob text,
	current 
	PRIMARY KEY (PatID)
);

CREATE TABLE IF NOT EXISTS alarm_information (
	msgCtrlID text,
	PatID int,
	reason map<text,text>,
	severness_counter <text,int>,
	PRIMARY KEY (PatID, msgCtrlID)
);

CREATE TYPE message_database.boundaries (
	lower int,
	upper int
);

CREATE TABLE IF NOT EXISTS patient_standard_values (
	msgCtrlID text,
	PatID int,
	parameters map<text, frozen <boundaries>>,
	current boolean,
	PRIMARY KEY (PatId)
);


