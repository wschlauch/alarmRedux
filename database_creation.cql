DROP KEYSPACE IF EXISTS message_database;

CREATE KEYSPACE message_database WITH REPLICATION =  {
	'class': 'SimpleStrategy',
	'replication_factor': 1	
};

use message_database;
CREATE TABLE IF NOT EXISTS oru_messages (
	msgCtrlID text,
	PatID int,
	numeric map<text, float>,
	textual map<text, text>,
	PRIMARY KEY (PatID, msgCtrlID)
);

CREATE TABLE IF NOT EXISTS adt_messages (
	msgCtrlID text,
	PatID int,
	dob text,
	current boolean,
	PRIMARY KEY (PatID)
);

CREATE TABLE IF NOT EXISTS alarm_information (
	msgCtrlID text,
	PatID int,
	reason map<text, text>,
	severness_counter map<text, int>,
	PRIMARY KEY (PatID, msgCtrlID)
);


CREATE TABLE IF NOT EXISTS patient_standard_values (
	tstamp timestamp,
	PatID int,
	parameters map<text, frozen <tuple<float,float>>>,
	current boolean,
	PRIMARY KEY (PatId)
);


